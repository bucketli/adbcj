/*
 *   Copyright (c) 2007 Mike Heath.  All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package org.adbcj.postgresql.codec;

import org.adbcj.*;
import org.adbcj.postgresql.codec.frontend.*;
import org.adbcj.support.AbstractDbSession;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.ExpectResultRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractConnection extends AbstractDbSession implements Connection {

	private final Logger logger = LoggerFactory.getLogger(AbstractConnection.class);

	private final AbstractConnectionManager connectionManager;
	private final ConnectionState connectionState;
	private Request<Void> closeRequest; // Access synchronized on lock

	private volatile int pid;
	private volatile int key;

	// Constant Messages
	private static final ExecuteMessage DEFAULT_EXECUTE = new ExecuteMessage();
	private static final BindMessage DEFAULT_BIND = new BindMessage();
	private static final DescribeMessage DEFAULT_DESCRIBE = DescribeMessage.createDescribePortalMessage(null);

	public AbstractConnection(AbstractConnectionManager connectionManager) {
		super(connectionManager.isPipeliningEnabled());
		this.connectionManager = connectionManager;
		this.connectionState = new ConnectionState(connectionManager.getDatabase());
	}

	public AbstractConnectionManager getConnectionManager() {
		return connectionManager;
	}

	public DbFuture<Void> ping() {
		// TODO Implement Postgresql ping
		throw new Error("ping() is not yet implemented");
	}

	@Override
	protected void checkClosed() {
		if (isClosed()) {
			throw new DbSessionClosedException(this, "This connection has been closed");
		}
	}

	public DbSessionFuture<Void> close(boolean immediate) throws DbException {
		// TODO AbstractConnection.finalizeClose(boolean) is almost identical to MySQL finalizeClose method, generify this

		// If the connection is already closed, return existing finalizeClose future
		synchronized (lock) {
			if (isClosed()) {
				if (closeRequest == null) {
					closeRequest = new Request<Void>(this) {
						@Override
						public void execute() throws Exception {
							// Do nothing since finalizeClose has already occurred
						}
						@Override
						public String toString() {
							return "Connection closed";
						}
					};
					closeRequest.setResult(null);
				}
			} else {
				if (immediate) {
					logger.debug("Executing immediate finalizeClose");
					// If the finalizeClose is immediate, cancel pending requests and send request to server
					cancelPendingRequests(true);
					write(SimpleFrontendMessage.TERMINATE);
					closeRequest = new Request<Void>(this) {
						@Override
						protected boolean cancelRequest(boolean mayInterruptIfRunning) {
							// Immediate finalizeClose can not be cancelled
							return false;
						}
						@Override
						public void execute() throws Exception {
							// Do nothing, finalizeClose message has already been sent
						}
						@Override
						public String toString() {
							return "Immediate finalizeClose";
						}
					};
				} else {
					// If the finalizeClose is NOT immediate, schedule the finalizeClose
					closeRequest = new Request<Void>(this) {
						@Override
						public boolean cancelRequest(boolean mayInterruptIfRunning) {
							logger.debug("Cancelling finalizeClose");
							unclose();
							return true;
						}
						@Override
						public void execute() {
							logger.debug("Sending TERMINATE to server (Request queue size: {})", requestQueue.size());
							write(SimpleFrontendMessage.TERMINATE);
						}
						@Override
						public boolean isPipelinable() {
							return false;
						}
						@Override
						public String toString() {
							return "Deferred finalizeClose";
						}
					};
					enqueueRequest(closeRequest);
				}
			}
			return closeRequest;
		}
	}

	private void unclose() {
		synchronized (lock) {
			logger.debug("Unclosing");
			this.closeRequest = null;
		}
	}

	public boolean isClosed() throws DbException {
		synchronized (lock) {
			return closeRequest != null || isConnectionClosing();
		}
	}

	public void finalizeClose() throws DbException {
		// TODO Make a DbSessionClosedException and use here
		errorPendingRequests(new DbException("Connection closed"));
		synchronized (lock) {
			if (closeRequest != null) {
				closeRequest.setResult(null);
			}
		}
	}

	public <T> DbSessionFuture<T> executeQuery(final String sql, ResultEventHandler<T> eventHandler, T accumulator) {
		checkClosed();
		Request<T> request = new ExpectResultRequest<T>(this,eventHandler, accumulator) {
			@Override
			public void execute() throws Exception {
				logger.debug("Issuing query: {}", sql);

				ParseMessage parse = new ParseMessage(sql);
				write(new AbstractFrontendMessage[] {
					parse,
					DEFAULT_BIND,
					DEFAULT_DESCRIBE,
					DEFAULT_EXECUTE,
					SimpleFrontendMessage.SYNC,
				});
			}
			@Override
			public String toString() {
				return "SELECT request: " + sql;
			}
		};
		return enqueueTransactionalRequest(request);
	}

	public DbSessionFuture<Result> executeUpdate(final String sql) {
		checkClosed();
		return enqueueTransactionalRequest(new Request<Result>(this) {
			@Override
			public void execute() throws Exception {
				logger.debug("Issuing update query: {}", sql);

				ParseMessage parse = new ParseMessage(sql);
				write(new AbstractFrontendMessage[] {
					parse,
					DEFAULT_BIND,
					DEFAULT_DESCRIBE,
					DEFAULT_EXECUTE,
					SimpleFrontendMessage.SYNC
				});
			}

			@Override
			public String toString() {
				return "Update request: " + sql;
			}
		});
	}

	public DbSessionFuture<PreparedStatement> prepareStatement(String sql) {
		// TODO Implement prepareStatement
		throw new IllegalStateException();
	}

	// ******** Transaction methods ***********************************************************************************

	private final AtomicLong statementCounter = new AtomicLong();
	private final Map<String, String> statementCache = Collections.synchronizedMap(new HashMap<String, String>());

	@Override
	protected void sendBegin() {
		executeStatement("BEGIN");
	}

	@Override
	protected void sendCommit() {
		executeStatement("COMMIT");
	}

	@Override
	protected void sendRollback() {
		executeStatement("ROLLBACK");
	}

	private void executeStatement(String statement) {
		String statementId = statementCache.get(statement);
		if (statementId == null) {
			long id = statementCounter.incrementAndGet();
			statementId = "S_" + id;

			ParseMessage parseMessage = new ParseMessage(statement, statementId);
			write(parseMessage);

			statementCache.put(statement, statementId);
		}
		write(new AbstractFrontendMessage[] {
				new BindMessage(statementId),
				DEFAULT_EXECUTE,
				SimpleFrontendMessage.SYNC
		});
	}

	// ================================================================================================================
	//
	// Non-API methods
	//
	// ================================================================================================================

	@Override
	protected <E> void enqueueRequest(Request<E> request) {
		super.enqueueRequest(request);
	}

	@Override
	public <E> Request<E> getActiveRequest() {
		return super.getActiveRequest();
	}

	public int getPid() {
		return pid;
	}

	public void setPid(int pid) {
		this.pid = pid;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public ConnectionState getConnectionState() {
		return connectionState;
	}

	// ================================================================================================================
	//
	// Abstract methods to be implemented by transport specific implementations
	//
	// ================================================================================================================

	protected abstract void write(AbstractFrontendMessage message);

	protected abstract void write(AbstractFrontendMessage[] messages);

	protected abstract boolean isConnectionClosing();

	public abstract DefaultDbFuture<Connection> getConnectFuture();

}

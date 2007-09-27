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
package org.safehaus.adbcj.mysql;

import org.apache.mina.common.IoSession;
import org.apache.mina.handler.demux.DemuxingIoHandler;
import org.safehaus.adbcj.DbException;
import org.safehaus.adbcj.support.AbstractDbFutureListenerSupport;
import org.safehaus.adbcj.support.DefaultDbFuture;
import org.safehaus.adbcj.support.Request;

public class MysqlProtocolHandler extends DemuxingIoHandler {
	
	@SuppressWarnings("unchecked")
	public MysqlProtocolHandler() {
		addMessageHandler(ServerGreeting.class, new ServerGreetingMessageHandler());
		addMessageHandler(OkResponse.class, new OkResponseMessageHandler());
		addMessageHandler(ErrorResponse.class, new ErrorResponseMessageHandler());
		
		// Add handler for result set messages
		ResultSetMessagesHandler resultSetMessagesHandler = new ResultSetMessagesHandler();
		addMessageHandler(ResultSetResponse.class, resultSetMessagesHandler);
		addMessageHandler(ResultSetFieldResponse.class, resultSetMessagesHandler);
		addMessageHandler(ResultSetRowResponse.class, resultSetMessagesHandler);
		addMessageHandler(EofResponse.class, resultSetMessagesHandler);
	}
	
	@Override
	public void sessionCreated(IoSession session) throws Exception {
		System.out.println("Session created"); // TODO Replace with logging
	}
	
	@Override
	public void sessionClosed(IoSession session) throws Exception {
		MysqlConnection connection = IoSessionUtil.getMysqlConnection(session);
		connection.setClosed(true);
		DefaultDbFuture<Void> closeFuture = connection.getCloseFuture();
		if (closeFuture != null) {
			closeFuture.setDone();
		}
		System.out.println("Session closed"); // TODO Replace with logging
	}
	
	@Override
	public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
		MysqlConnection connection = IoSessionUtil.getMysqlConnection(session);
		Request<?> activeRequest = connection.getActiveRequest();
		if (activeRequest == null) {
			// TODO Figure out what to do with the exception
			cause.printStackTrace();
		} else {
			AbstractDbFutureListenerSupport<?> future = activeRequest.getFuture();
			if (future != null) {
				if (cause instanceof DbException) {
					future.setException((DbException)cause);
				} else {
					future.setException(new DbException(cause));
				}
				future.setDone();
				connection.makeNextRequestActive();
			}
		}
	}
	
}
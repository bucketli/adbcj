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
package org.adbcj.tck.test;

import org.adbcj.*;
import org.adbcj.tck.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(invocationCount = 50, threadPoolSize = 10, timeOut = 30000)
public class ConnectTest {
	
	private final Logger logger = LoggerFactory.getLogger(ConnectTest.class);

	private ConnectionManager connectionManager;

	@Parameters({"url", "user", "password"})
	@BeforeTest
	public void createConnectionManager(String url, String user, String password) {
		connectionManager = ConnectionManagerProvider.createConnectionManager(url, user, password);
	}

	@AfterTest
	public void closeConnectionManager() {
		DbFuture<Void> closeFuture = connectionManager.close(true);
		closeFuture.getUninterruptably();
	}

	public void testConnectImmediateClose() throws Exception {
		final boolean[] callbacks = {false, false};
		final CountDownLatch latch = new CountDownLatch(2);

		DbFuture<Connection> connectFuture = connectionManager.connect().addListener(new DbListener<Connection>() {
			public void onCompletion(DbFuture<Connection> future) throws Exception {
				// Indicate that callback has been invoked
				callbacks[0] = true;
				latch.countDown();
			}
		});
		Connection connection = connectFuture.get(5, TimeUnit.SECONDS);
		assertTrue(!connection.isClosed());
		DbFuture<Void> closeFuture = connection.close(true).addListener(new DbListener<Void>() {
			public void onCompletion(DbFuture<Void> future) throws Exception {
				// Indicate that callback has been invoked
				callbacks[1] = true;
				latch.countDown();
			}
		});
		closeFuture.get(5, TimeUnit.SECONDS);
		assertTrue(connection.isClosed());
		latch.await(1, TimeUnit.SECONDS);
		assertTrue(callbacks[0], "Callback on connection future was not invoked");
		assertTrue(callbacks[1], "Callback on finalizeClose future was not invoked");
	}

	public void testConnectNonImmediateClose() throws DbException, InterruptedException {
		final boolean[] callbacks = {false};
		final CountDownLatch latch = new CountDownLatch(1);

		Connection connection = connectionManager.connect().get();
		assertTrue(!connection.isClosed());
		connection.close(true).addListener(new DbListener<Void>() {
			public void onCompletion(DbFuture<Void> future) throws Exception {
				// Indicate that finalizeClose callback has been invoked
				callbacks[0] = true;
				latch.countDown();
			}
		}).get();
		assertTrue(connection.isClosed());
		latch.await(1, TimeUnit.SECONDS);
		assertTrue(callbacks[0], "Callback on finalizeClose future was not invoked");
	}

	public void testCancelClose() throws DbException, InterruptedException {
		final AtomicBoolean[] closeCallback = {new AtomicBoolean(),new AtomicBoolean()};

		// This connection is used for doing a select for update lock
		Connection lockConnection = connectionManager.connect().get();
		Connection connectionToClose = connectionManager.connect().get();

		try {
			// Get lock with select for update
			lockConnection.beginTransaction();
			TestUtils.selectForUpdate(lockConnection).get();

			// Do select for update on second connection so we can finalizeClose it and then cancel the finalizeClose
			connectionToClose.beginTransaction();
			DbFuture<ResultSet> future = TestUtils.selectForUpdate(connectionToClose);

			DbSessionFuture<Void> closeFuture = connectionToClose.close(false).addListener(new DbListener<Void>() {
				public void onCompletion(DbFuture<Void> future) throws Exception {
					logger.debug("testCancelClose: In finalizeClose callback for connectionManager {}", connectionManager);
					closeCallback[0].set(true);
					closeCallback[1].set(future.isCancelled());
				}
			});
			assertTrue(connectionToClose.isClosed(), "This connection should be flagged as closed now");
            if(!(closeFuture.isDone() || closeFuture.cancel(false))){
                while(!closeFuture.cancel(false)){
                    System.out.println("wtf");
                }
                System.out.println("wtf2");
            }
			assertTrue(closeFuture.isDone() || closeFuture.cancel(false), "The connection finalizeClose should have cancelled properly");
			assertFalse(connectionToClose.isClosed(), "This connection should not be closed because we canceled the finalizeClose");

			// Release lock
			lockConnection.rollback().get();

			// Make sure closingConnection's select for update completed successfully
			future.get();
			connectionToClose.rollback().get();
		} finally {
			if (lockConnection.isInTransaction()) {
				lockConnection.rollback().get();
			}
			if (!connectionToClose.isClosed() && connectionToClose.isInTransaction()) {
				connectionToClose.rollback().get();
			}

			lockConnection.close(true);
			connectionToClose.close(true);
		}
		// Make sure the finalizeClose's callback was invoked properly
		assertTrue(closeCallback[0].get(), "The finalizeClose callback was not invoked when cancelled");
		assertTrue(closeCallback[1].get(), "The finalizeClose future did not indicate the finalizeClose was cancelled");
	}
	
	public void testNonImmediateClose() throws Exception {
		Connection connection = connectionManager.connect().get();

		List<DbSessionFuture<ResultSet>> futures = new ArrayList<DbSessionFuture<ResultSet>>();

		for (int i = 0; i < 5; i++) {
			futures.add(connection.executeQuery(String.format("SELECT *, %d FROM simple_values", i)));
		}
		try {
			connection.close(false).get(10, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			for (DbSessionFuture<ResultSet> future : futures) {
				if (future.isDone()) {
					future.get(); // Will throw exception if failed
				} else {
					throw new AssertionError("future " + future + " did not complete in time");
				}
			}
			throw new AssertionError("finalizeClose future failed to complete");
		}
		assertTrue(connection.isClosed(), "Connection should be closed");
		for (DbSessionFuture<ResultSet> future : futures) {
			assertTrue(future.isDone(), "Request did not finish before connection was closed: " + future);
			assertFalse(future.isCancelled(), "Future was cancelled and should have been");
		}
	}

}

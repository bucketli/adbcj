package org.adbcj.poolx;

import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerFactory;
import org.adbcj.DbException;

import java.util.Map;

/**
 * @author roman.stoffel@gamlor.info
 */
public class MockConnectionManagerFactory implements ConnectionManagerFactory {
    private static ThreadLocal<MockConnectionManager> lastInstance = new ThreadLocal<MockConnectionManager>();
    @Override
    public ConnectionManager createConnectionManager(String url,
                                                     String username,
                                                     String password,
                                                     Map<String, String> properties) throws DbException {
        final MockConnectionManager instance = new MockConnectionManager();
        lastInstance.set(instance);
        return instance;
    }

    public static MockConnectionManager lastInstanceRequestedOnThisThread(){
        return lastInstance.get();
    }

    @Override
    public boolean canHandle(String protocol) {
        return "mock".equals(protocol);
    }

}

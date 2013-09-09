package org.adbcj.poolx;

import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerProvider;
import org.testng.annotations.Test;

/**
 * @author foooling@gmail.com
 *         13-9-9
 */
public class PoolxConnectionTest {
    @Test
    public void testConnection(){
        final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager("adbcj:poolx:mock:database", "sa", "pwd");
    }
}

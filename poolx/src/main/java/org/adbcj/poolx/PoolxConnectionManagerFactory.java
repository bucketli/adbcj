package org.adbcj.poolx;

import org.adbcj.ConnectionManager;
import org.adbcj.ConnectionManagerFactory;
import org.adbcj.ConnectionManagerProvider;
import org.adbcj.DbException;

import java.util.Map;

/**
 * @author foooling@gmail.com
 *         13-9-6
 */
public class PoolxConnectionManagerFactory implements ConnectionManagerFactory {
    private static final String PROTOCOL = "poolx";

    @Override
    public ConnectionManager createConnectionManager(String url, String username,
                                                     String password,
                                                     Map<String, String> properties) throws DbException {
        final String[] firstAndSecondPart = url.split("poolx:");
        if(firstAndSecondPart.length!=2){
            throw new IllegalArgumentException("Expect a URL in the form of adbcj:[pooltype]:[driver]:[database-url]. Got: "+url);
        }
        String nativeUrl = firstAndSecondPart[0] + firstAndSecondPart[1];

        return new PoolxConnectionManager(
                ConnectionManagerProvider.createConnectionManager(nativeUrl, username, password, properties),
                properties,new ConfigInfo((properties)));
    }

    @Override
    public boolean canHandle(String protocol) {
        return PROTOCOL.equals(protocol.toLowerCase());
    }
}

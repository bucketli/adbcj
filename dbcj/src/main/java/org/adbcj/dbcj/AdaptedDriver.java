package org.adbcj.dbcj;

import java.net.URI;
import java.sql.*;

import org.adbcj.ConnectionManagerProvider;

import java.util.Collections;
import java.util.logging.Logger;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.adbcj.ConnectionManager;
import org.adbcj.DbException;

/**
 * @author foooling@gmail.com
 */
public class AdaptedDriver implements java.sql.Driver {

    public static final String DBCJ_PROTOCOL="jdbc";
    protected ConcurrentHashMap<String,ConnectionManager>managerConcurrentHashMap=new ConcurrentHashMap<String, ConnectionManager>();
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        org.adbcj.Connection realConnection;
        StringBuilder stringBuilder=new StringBuilder();

        String user=info.getProperty("user");
        String password=info.getProperty("password");


        String adaptedUrl=adaptUrl(url).toString();
        // [+-+] is used to split strings , decrease rate of collision
        // TODO: not work in special situations , multi-key is better
        stringBuilder.append(adaptedUrl).append("+-+").append(user).append("+-+").append(password);
        String key= stringBuilder.toString();
        ConnectionManager connectionManager=managerConcurrentHashMap.get(key);

        if (connectionManager==null){
            connectionManager=ConnectionManagerProvider.createConnectionManager(adaptedUrl,user,password);
            managerConcurrentHashMap.putIfAbsent(key,connectionManager);
        }else if (connectionManager.isClosed()){
            connectionManager=ConnectionManagerProvider.createConnectionManager(adaptedUrl,user,password);
            managerConcurrentHashMap.put(key,connectionManager);
        }
        try{
            realConnection=connectionManager.connect().get();
        }catch (Exception e){
            throw new SQLException("connection establish fail");
        }

        return new ConnectionImpl(realConnection);
    }
    private StringBuffer adaptUrl(String url) throws SQLException{

        URI uri =null;
        try{
            uri=new URI(url);
        } catch (Exception e){
            throw new SQLException("Invalid connection URL: " + url);
        }
        String dbcjProtocol = uri.getScheme();
        if (!DBCJ_PROTOCOL.equals(dbcjProtocol)) {
            throw new DbException("Invalid connection URL: " + url);
        }
        final String[] firstAndSecondPart = url.split(DBCJ_PROTOCOL);
        if(firstAndSecondPart.length<2){
            throw new SQLException("Invalid connection URL: " + url);
        }
        //TODO: to make this configurable
        StringBuffer stringBuffer=new StringBuffer("adbcj");
        stringBuffer.append(firstAndSecondPart[1]);
        return stringBuffer;

    }
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMajorVersion() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMinorVersion() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean jdbcCompliant() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

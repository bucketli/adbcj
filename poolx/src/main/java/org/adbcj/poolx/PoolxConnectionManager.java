package org.adbcj.poolx;

import org.adbcj.*;
import org.adbcj.support.AbstractConnectionManager;
import org.adbcj.support.DefaultDbFuture;
import org.adbcj.support.OneArgFunction;
import org.adbcj.support.stacktracing.StackTracingOptions;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.adbcj.support.FutureUtils;
/**
 * @author foooling@gmail.com
 *         13-9-6
 */
public class PoolxConnectionManager extends AbstractConnectionManager {
    protected ConnectionManager connectionManager;
    protected ConfigInfo config;
    protected List<Connection> realConnections=new ArrayList<Connection>();
    protected List<PoolxConnection> virtualConnections=new LinkedList<PoolxConnection>();
    protected AtomicInteger allocatedConnectionsCount=new AtomicInteger();
    Object lock=new Object();
    protected final AtomicInteger roundCounter=new AtomicInteger(0);


    private final Timer timeOutTimer = new Timer("PooledConnectionManager timeout timer",true);

    public PoolxConnectionManager(ConnectionManager connectionManager,Map<String, String> properties,ConfigInfo config) {
        super(properties);
        this.connectionManager=connectionManager;
        this.config=config;
    }



    @Override
    public DbFuture<Connection> connect() {
        //TODO unchecked
        if (isClosed()){
            throw new DbException("ConnectionManager is closed. Failed to open new connections.");
        }

        synchronized(lock){
            if ((int)config.getMaxConnections()>realConnections.size()){
                try {
                    realConnections.add(connectionManager.connect().get());
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        };

        Connection vconn=new PoolxConnection(this);
        return DefaultDbFuture.completed(vconn);
    }

    @Override
    public DbFuture<Void> close() {
        return super.close();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    protected DbFuture<Void> doClose(CloseMode mode) {
        timeOutTimer.cancel();
        for (Connection virtualConnection : virtualConnections) {
            virtualConnection.close(mode);
        }
        return connectionManager.close(mode);
    }

    @Override
    public int maxQueueLength() {
        return super.maxQueueLength();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public StackTracingOptions stackTracingOptions() {
        return super.stackTracingOptions();    //To change body of overridden methods use File | Settings | File Templates.
    }


    public Connection dispatcher(){
        return dispatcher(false);
    }
    public Connection dispatcher(boolean needLock){
        if (!needLock){
            return realConnections.get(roundCounter.getAndIncrement()%realConnections.size());
        }
        return null;
    }
}

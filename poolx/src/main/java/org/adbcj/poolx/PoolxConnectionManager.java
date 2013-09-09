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
    protected List<Connection> realConnections=new LinkedList<Connection>();
    protected List<PoolxConnection> virtualConnections=new LinkedList<PoolxConnection>();
    protected AtomicInteger allocatedConnectionsCount=new AtomicInteger();


    private final Timer timeOutTimer = new Timer("PooledConnectionManager timeout timer",true);

    public PoolxConnectionManager(ConnectionManager connectionManager,Map<String, String> properties,ConfigInfo config) {
        super(properties);
        this.connectionManager=connectionManager;
        this.config=config;
    }


    /*
    *  init MaxConnection number of real connections
    *
    */
    public DbFuture<Boolean> init(){
        //TODO unchecked , no timeout yet
        final AtomicLong finishCount=new AtomicLong(0);
        final DbFuture<Boolean> dbFuture=new DefaultDbFuture<Boolean>(stackTracingOptions());
        for (long i=0;i<config.getMaxConnections();i++){
            connectionManager.connect().addListener(new DbListener<Connection>() {
                @Override
                public void onCompletion(DbFuture<Connection> future) {
                    try{
                        realConnections.add(future.get());
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                    long nowCount =finishCount.addAndGet(1);
                    if(nowCount==config.getMaxConnections()){
                        ((DefaultDbFuture)dbFuture).setResult(true);
                    }
                }
            });
        }
        return dbFuture;

    }

    @Override
    public DbFuture<Connection> connect() {
        //TODO unchecked
        if (isClosed()){
            throw new DbException("ConnectionManager is closed. Failed to open new connections.");
        }
        for (Connection realConnection:realConnections){
            if (!realConnection.isInTransaction()){
                Connection vconn=new PoolxConnection(this);
                return DefaultDbFuture.completed(vconn);
            }
        }
        throw new DbException("All pooled connections are in transaction, Cannot create more connections now.");
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
}

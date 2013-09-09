package org.adbcj.poolx;
import org.adbcj.*;

/**
 * @author foooling@gmail.com
 *         13-9-6
 */
public class PoolxConnection implements Connection {
    protected final PoolxConnectionManager poolxConnectionManager;
    protected Connection realConnection=null;
    public PoolxConnection(PoolxConnectionManager poolxConnectionManager){
        this.poolxConnectionManager=poolxConnectionManager;

    }
    @Override
    public ConnectionManager getConnectionManager() {
        return poolxConnectionManager;
    }

    @Override
    public void beginTransaction() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DbFuture<Void> commit() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DbFuture<Void> rollback() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isInTransaction() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DbFuture<ResultSet> executeQuery(String sql) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> DbFuture<T> executeQuery(String sql, ResultHandler<T> eventHandler, T accumulator) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DbFuture<Result> executeUpdate(String sql) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DbFuture<PreparedQuery> prepareQuery(String sql) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DbFuture<PreparedUpdate> prepareUpdate(String sql) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DbFuture<Void> close() throws DbException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DbFuture<Void> close(CloseMode closeMode) throws DbException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isClosed() throws DbException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isOpen() throws DbException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

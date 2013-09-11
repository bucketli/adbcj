package org.adbcj.poolx;

import org.adbcj.DbFuture;
import org.adbcj.PreparedQuery;
import org.adbcj.ResultHandler;
import org.adbcj.ResultSet;

/**
 * @author foooling@gmail.com
 *         13-9-10
 */
public class PoolxPreparedQuery extends AbstractPoolxPreparedStatement implements PreparedQuery {
    public PoolxPreparedQuery(PoolxConnection poolxConnection,String sql){
        super(poolxConnection,sql);
    }
    @Override
    public DbFuture<ResultSet> execute(Object... params) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> DbFuture<T> executeWithCallback(ResultHandler<T> eventHandler, T accumulator, Object... params) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

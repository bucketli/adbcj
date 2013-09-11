package org.adbcj.poolx;
import org.adbcj.DbFuture;
import org.adbcj.PreparedUpdate;
import org.adbcj.Result;

/**
 * @author foooling@gmail.com
 *         13-9-10
 */
public class PoolxPreparedUpdate extends AbstractPoolxPreparedStatement implements PreparedUpdate{
    public PoolxPreparedUpdate(PoolxConnection poolxConnection,String sql){
        super(poolxConnection,sql);
    }
    @Override
    public DbFuture<Result> execute(Object... params) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

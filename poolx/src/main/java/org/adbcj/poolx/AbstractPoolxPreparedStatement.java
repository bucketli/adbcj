package org.adbcj.poolx;
import org.adbcj.DbFuture;
import org.adbcj.PreparedStatement;
import org.adbcj.support.DefaultDbFuture;


/**
 * @author foooling@gmail.com
 *         13-9-10
 */
public class AbstractPoolxPreparedStatement implements PreparedStatement {
    protected final PoolxConnection virtualConnection ;
    protected String preparedSql;
    protected volatile DbFuture<Void> closeFuture=null;
    public AbstractPoolxPreparedStatement(PoolxConnection poolxConnection,String sql){
        virtualConnection=poolxConnection;
        preparedSql=sql;
    }
    @Override
    public boolean isClosed() {
        return closeFuture!=null;
    }

    @Override
    public DbFuture<Void> close() {
        synchronized (this){
            if (closeFuture !=null){
                return closeFuture;
            }
            closeFuture= DefaultDbFuture.completed(null);
            return closeFuture;
        }
    }
}

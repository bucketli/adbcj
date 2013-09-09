package poolx;

import org.adbcj.*;

/**
 * @author foooling@gmail.com
 *         13-9-9
 */
public class PoolxDemo {
    public static void main(String[] args){
        final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager(
                "adbcj:poolx:mysql://localhost/adbcjtck",
                "adbcjtck",
                "adbcjtck"
        );

        // Connect to your database. It's asynchronous.
        // This means we react on it when done
        final DbFuture<Connection> connect = connectionManager.connect();
        connect.addListener(new DbListener<Connection>() {
            @Override
            public void onCompletion(DbFuture<Connection> connectionDbFuture) {
                switch (connectionDbFuture.getState()){
                    case SUCCESS:
                        final Connection connection = connectionDbFuture.getResult();
                        break;
                    case FAILURE:
                        connectionDbFuture.getException().printStackTrace();
                        break;
                    case CANCELLED:
                        System.out.println("Cancelled");
                        break;
                }
            }
        });
        try{

            connect.get();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}

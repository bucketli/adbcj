package poolx;

import org.adbcj.*;

import java.util.*;

/**
 * @author foooling@gmail.com
 *         13-9-9
 */
public class PoolxD {
    public static void main(String[] args){
        final Map<String,String> properties=new HashMap<String, String>();
        properties.put("pool.maxConnections","2");
        final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager(
                "adbcj:poolx:mysql://localhost/adbcjtck",
                "adbcjtck",
                "adbcjtck",properties
        );
        List<Thread> threads=new LinkedList<Thread>();
        for (int i=0;i<3;i++){
            Thread td=new Thread(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                    DbFuture<Connection> connect = connectionManager.connect();
                    connect.addListener(new DbListener<Connection>() {
                        @Override
                        public void onCompletion(DbFuture<Connection> future) {
                            switch (future.getState()){
                                case SUCCESS:
                                    final Connection connection = future.getResult();
                                    continueWithInserting(connection);
                                    break;
                                case FAILURE:
                                    future.getException().printStackTrace();
                                    break;
                                case CANCELLED:
                                    System.out.println("Cancelled");
                                    break;
                            }
                        }
                    });
                }
            });
            td.start();
            threads.add(td);
        }

        Iterator<Thread> iterator=threads.iterator();
        while(iterator.hasNext()){
            try {
                iterator.next().join();
            } catch (Exception e){
                e.printStackTrace();
            }
        }



    }
    private static void continueAndCreateSchema(final Connection connection) {
        // Again, we send the query and add a listener to react to it
        connection.executeUpdate("CREATE TABLE IF NOT EXISTS posts(\n" +
                "  id int NOT NULL AUTO_INCREMENT,\n" +
                "  title varchar(255) NOT NULL,\n" +
                "  content TEXT NOT NULL,\n" +
                "  PRIMARY KEY (id)\n" +
                ") ENGINE = INNODB;").addListener(new DbListener<Result>() {
            @Override
            public void onCompletion(DbFuture<Result> resultDbFuture) {
                switch (resultDbFuture.getState()) {
                    case SUCCESS:
                        System.out.println("Created Schema, Inserting");
                        continueWithInserting(connection);
                        break;
                    case FAILURE:
                        resultDbFuture.getException().printStackTrace();
                        break;
                    case CANCELLED:
                        System.out.println("Cancelled");
                        break;
                }
            }
        });
    }

    private static void continueWithInserting(final Connection connection) {
        // We can directly send multiple queries
        // And then wait until everyone is done.
        final DbFuture<Result> firstPost = connection.executeUpdate("INSERT INTO posts(title,content) VALUES('The Title','TheContent')");
        final DbFuture<Result> secondPost = connection.executeUpdate("INSERT INTO posts(title,content) VALUES('Second Title','More Content')");
        final DbFuture<Result> thirdPost = connection.executeUpdate("INSERT INTO posts(title,content) VALUES('Third Title','Even More Content')");
        final DbListener<Result> allDone = new DbListener<Result>() {
            @Override
            public void onCompletion(DbFuture<Result> resultSetDbFuture) {
                switch (resultSetDbFuture.getState()) {
                    case SUCCESS:
                        // Check if everyone is done
                        if(firstPost.isDone()&&secondPost.isDone()&&thirdPost.isDone()){
                            continueWithSelect(connection);
                        }
                        break;
                    case FAILURE:
                        resultSetDbFuture.getException().printStackTrace();
                        break;
                    case CANCELLED:
                        System.out.println("Cancelled");
                        break;
                }

            }
        };
        // Register the listener to all instances
        firstPost.addListener(allDone);
        secondPost.addListener(allDone);
        thirdPost.addListener(allDone);
    }

    private static void continueWithSelect(final Connection connection) {
        connection.executeQuery("SELECT * FROM posts").addListener(new DbListener<ResultSet>() {
            @Override
            public void onCompletion(DbFuture<ResultSet> resultSetDbFuture) {
                switch (resultSetDbFuture.getState()) {
                    case SUCCESS:
                        listResultSet(resultSetDbFuture.getResult());

                        // result sets are immutable. You can close the connection
                        connection.close();
                        break;
                    case FAILURE:
                        resultSetDbFuture.getException().printStackTrace();
                        break;
                    case CANCELLED:
                        System.out.println("Cancelled");
                        break;
                }
            }
        });
    }

    private static void listResultSet(ResultSet result) {
        for (Row row : result) {
            System.out.println("ID: "+row.get("ID").getLong()+" with title "+row.get("title").getString());
        }
    }
}

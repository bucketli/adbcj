package info.adbcj.demo;

import org.adbcj.*;

/**
 * @author foooling@gmail.com
 *         13-9-10
 */
public class StmtDemo {
    public static void main(String[] args){
        final ConnectionManager connectionManager = ConnectionManagerProvider.createConnectionManager(
                "adbcj:mysql://localhost/adbcjtck",
                "adbcjtck",
                "adbcjtck"
        );
        final DbFuture<Connection> connect = connectionManager.connect();
        connect.addListener(new DbListener<Connection>() {
            @Override
            public void onCompletion(DbFuture<Connection> future) {
                switch (future.getState()){
                    case SUCCESS:
                        final Connection connection = future.getResult();
                        continueAndCreateSchema(connection);
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
        final DbFuture<PreparedUpdate> preparedUpdateDbFuture=connection.prepareUpdate("INSERT INTO posts(title,content) VALUES(?,?)");
        preparedUpdateDbFuture.addListener(new DbListener<PreparedUpdate>() {
            @Override
            public void onCompletion(DbFuture<PreparedUpdate> future) {
                switch (future.getState()) {
                    case SUCCESS:
                        System.out.println("Created Statement");
                        try {
                            PreparedUpdate preparedUpdate=future.get();
                            final DbFuture<Result> firstPost = preparedUpdate.execute("The Title","TheContent");
                            final DbFuture<Result> secondPost = preparedUpdate.execute("Second Title","More Content");
                            final DbFuture<Result> thirdPost = preparedUpdate.execute("Third Title","And More Content");
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
                        } catch (Exception e){
                            e.printStackTrace();
                        }

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

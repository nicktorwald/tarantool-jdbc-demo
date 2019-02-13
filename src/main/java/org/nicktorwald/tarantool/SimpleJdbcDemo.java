package org.nicktorwald.tarantool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class SimpleJdbcDemo implements Runnable {

    private static final String DROP_TABLE_ALIAS = "DROP TABLE";
    private static final String CREATE_TABLE_ALIAS = "CREATE TABLE";
    private static final String INSERT_INTO_ALIAS = "INSERT INTO";
    private static final String SELECT_ONE_ALIAS = "SELECT ONE";
    private final String connectionString;
    private Map<String, String> initSqlDictionary = new HashMap<>();
    private Map<String, String> opSqlDictionary = new HashMap<>();

    public SimpleJdbcDemo(String connectionString) {
        this.connectionString = connectionString;
        initDictionaries();
    }

    private void initDictionaries() {
        initSqlDictionary.put(
                DROP_TABLE_ALIAS,
                "DROP TABLE car"
        );
        initSqlDictionary.put(
                CREATE_TABLE_ALIAS,
                "CREATE TABLE car (id int NOT NULL PRIMARY KEY, vendor varchar(255) NOT NULL)"
        );
        opSqlDictionary.put(
                INSERT_INTO_ALIAS,
                "INSERT INTO car (id, vendor) VALUES (?, ?)"
        );
        opSqlDictionary.put(
                SELECT_ONE_ALIAS,
                "SELECT * FROM car WHERE id = ?"
        );
    }

    @Override
    public void run() {
        try (Connection jdbcConnection = getJdbcConnection()) {
            prepareScheme(jdbcConnection);
            excecuteQueries(jdbcConnection);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    private void excecuteQueries(Connection jdbcConnection) {
        try (PreparedStatement insertStatement = jdbcConnection.prepareStatement(lookupOpQuery(INSERT_INTO_ALIAS));
             PreparedStatement selectStatement = jdbcConnection.prepareStatement(lookupOpQuery(SELECT_ONE_ALIAS))) {

            insertStatement.setInt(1, 1);
            insertStatement.setString(2, "Toyota");
            System.out.println(insertStatement.executeUpdate());

            selectStatement.setInt(1, 1);
            try (ResultSet resultSet = selectStatement.executeQuery()) {
                while (resultSet.next()) {
                    System.out.print(resultSet.getInt(1));
                    System.out.print(", ");
                    System.out.print(resultSet.getString(2));
                    System.out.println();
                }
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    private void prepareScheme(Connection jdbcConnection) {
        try (Statement dropStatement = jdbcConnection.createStatement()) {
            System.out.println(dropStatement.executeUpdate(lookupInitQuery(DROP_TABLE_ALIAS)));
        } catch (Exception ignored) {
        }
        try (Statement createStatement = jdbcConnection.createStatement()) {
            System.out.println(createStatement.executeUpdate(lookupInitQuery(CREATE_TABLE_ALIAS)));
        } catch (Exception ignored) {
        }
    }

    private String lookupInitQuery(String lookup) {
        return initSqlDictionary.get(lookup);
    }

    private String lookupOpQuery(String lookup) {
        return opSqlDictionary.get(lookup);
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(connectionString);
    }

}

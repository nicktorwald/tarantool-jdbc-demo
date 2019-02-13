package org.nicktorwald.tarantool;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleSpringJdbcDemo implements Runnable {

    private static final String DROP_TABLE_ALIAS = "DROP TABLE";
    private static final String CREATE_TABLE_ALIAS = "CREATE TABLE";
    private static final String INSERT_INTO_ALIAS = "INSERT INTO";
    private static final String SELECT_ONE_ALIAS = "SELECT ONE";
    private final String connectionString;
    private Map<String, String> initSqlDictionary = new HashMap<>();
    private Map<String, String> opSqlDictionary = new HashMap<>();

    public SimpleSpringJdbcDemo(String connectionString) {
        this.connectionString = connectionString;
        initDictionaries();
    }

    private void initDictionaries() {
        initSqlDictionary.put(
                DROP_TABLE_ALIAS,
                "DROP TABLE employee"
        );
        initSqlDictionary.put(
                CREATE_TABLE_ALIAS,
                "CREATE TABLE employee (id int NOT NULL PRIMARY KEY, name varchar(255) NOT NULL)"
        );
        opSqlDictionary.put(
                INSERT_INTO_ALIAS,
                "INSERT INTO employee (id, name) VALUES (:id, :name)"
        );
        opSqlDictionary.put(
                SELECT_ONE_ALIAS,
                "SELECT * FROM employee WHERE id = :id"
        );
    }

    @Override
    public void run() {
        NamedParameterJdbcTemplate jdbcTemplate = getJdbcTemplate();
        RowMapper<Object> rowMapper = (resultSet, rowNumber) -> Arrays.asList(resultSet.getInt(1), resultSet.getString(2));

        prepareScheme(jdbcTemplate);

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("id", 1);
        params.put("name", "John Smith");

        System.out.println(jdbcTemplate.update(lookupOpQuery(INSERT_INTO_ALIAS), params));
        System.out.println(jdbcTemplate.query(lookupOpQuery(SELECT_ONE_ALIAS), Collections.singletonMap("id", 1), rowMapper));
    }

    private void prepareScheme(NamedParameterJdbcTemplate jdbcTemplate) {
        try {
            System.out.println(jdbcTemplate.update(lookupInitQuery(DROP_TABLE_ALIAS), Collections.emptyMap()));
        } catch (Exception ignored) {
        }
        System.out.println(jdbcTemplate.update(lookupInitQuery(CREATE_TABLE_ALIAS), Collections.emptyMap()));
    }

    private String lookupInitQuery(String lookup) {
        return initSqlDictionary.get(lookup);
    }

    private String lookupOpQuery(String lookup) {
        return opSqlDictionary.get(lookup);
    }

    private NamedParameterJdbcTemplate getJdbcTemplate() {
        return new NamedParameterJdbcTemplate(new DriverManagerDataSource(connectionString));
    }

}

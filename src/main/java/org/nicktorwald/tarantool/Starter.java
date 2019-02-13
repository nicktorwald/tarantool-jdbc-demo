package org.nicktorwald.tarantool;

public class Starter {

    private static final String CONNECTION_STRING = "tarantool://localhost:3301";

    public static void main(String[] args) {
        new SimpleSpringJdbcDemo(CONNECTION_STRING).run();
        new SimpleJdbcDemo(CONNECTION_STRING).run();
    }

}

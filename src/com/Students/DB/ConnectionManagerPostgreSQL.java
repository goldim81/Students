package com.Students.DB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionManagerPostgreSQL implements IConnectionManager{
    private static final ConnectionManagerPostgreSQL INSTANCE = new ConnectionManagerPostgreSQL();
    private Connection connection;

    private ConnectionManagerPostgreSQL() {

        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/StudentDB", "postgres", "Qwerty123");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static synchronized ConnectionManagerPostgreSQL getInstance() {
        return INSTANCE;
    }

    public Connection getConnection() {
        return connection;
    }

}

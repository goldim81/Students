package com.Students.Object;

import com.Students.Object.Student;

import java.sql.*;

public class DatabaseManager {
    public static void main(String[] args) throws ClassNotFoundException {
        showStudents();
    }
    public static void showStudents () throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        Connection connection ;
        try {
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/SrudentDB",
                    "postgres",
                    "Qwerty123");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM public.\"Student\"");
            while (resultSet.next()) {
                Student student = new Student(resultSet.getString("firstName"), resultSet.getString("secondName"),
                        resultSet.getString("lastName"), resultSet.getDate("birthday").toLocalDate(), resultSet.getShort("id"));
                System.out.println(student);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}

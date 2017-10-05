package com.Students.DB.DAO;

import com.Students.DB.ConnectionManagerPostgreSQL;
import com.Students.DB.IConnectionManager;
import com.Students.Object.Group;
import com.Students.Object.Student;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StudentDAO implements IDAO <Student>{

    public static class StudentDAOException extends Exception {

    }

    private static IConnectionManager manager;

    static {
        manager = ConnectionManagerPostgreSQL.getInstance();
    }

    /**
     * Метод возвращает список студентов из БД
     * @return List<Student> список сдудентов
     * @throws StudentDAOException
     */
    @Override
    public List<Student> getAll() throws StudentDAOException {
        List<Student> studentList = new ArrayList<>();
        Statement statement = null;
        try {
            statement = manager.getConnection().createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT st.*, gr.name\n" +
                    "FROM public.\"Student\" st LEFT JOIN public.\"Group\" gr ON st.\"groupId\" = gr.id;");
            while (resultSet.next()) {
                Student student = new Student(resultSet.getShort("id"), resultSet.getString("firstName"), resultSet.getString("secondName"),
                        resultSet.getString("lastName"), resultSet.getDate("birthday").toLocalDate());
//                student.setGroup(new Group());
                studentList.add(student);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new StudentDAOException();
        }
        return studentList;
    }

    @Override
    public Student getByID(int id) throws StudentDAOException {
        PreparedStatement statement = null;
        try {
            statement = manager.getConnection().prepareStatement("SELECT * FROM public.\"Student\" WHERE id = ? ");
            statement.setInt(1, id);
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            return new Student(resultSet.getInt("id"), resultSet.getString("firstName"), resultSet.getString("secondName"),
                    resultSet.getString("lastName"), resultSet.getDate("birthday").toLocalDate());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new StudentDAOException();
        }
    }

    private PreparedStatement getUpdateStatement() throws SQLException {
        return manager.getConnection().prepareStatement(
                "UPDATE public.\"student\" " +
                        "SET  firstName = ?, lastName = ?, secondName = ?, birthday = ?, groupId = ?" +
                        "WHERE id = ? ");
    }
    @Override
    public void update(Student obj) throws StudentDAOException {
        PreparedStatement statement = null;
        try {
            statement = getUpdateStatement();
            statement.setString(1, obj.getFirstName());
            statement.setString(2, obj.getFamilyName());
            statement.setString(3, obj.getSecondName());
            statement.setDate(4, Date.valueOf(obj.getBdate()));
            statement.setInt(5, obj.getGroup().getId());
            statement.setInt(6, obj.getId());
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new StudentDAOException();
        }
    }

    @Override
    public void updateAll(List<Student> objList) throws StudentDAOException {
        PreparedStatement statement = null;
        try {
            statement = getUpdateStatement();
            for (Student student : objList) {
                statement.setString(1, student.getFirstName());
                statement.setString(2, student.getFamilyName());
                statement.setString(3, student.getSecondName());
                statement.setDate(4, Date.valueOf(student.getBdate()));
                statement.setInt(5, student.getGroup().getId());
                statement.setInt(6, student.getId());
                statement.addBatch();

            }
            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new StudentDAOException();
        }
    }

    @Override
    public void deleteByID(int id) throws StudentDAOException {
        PreparedStatement statement = null;
        try {
            statement = manager.getConnection().prepareStatement(
                    "DELETE public.\"student\" WHERE id = ? ");
            statement.setInt(1, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new StudentDAOException();
        }
    }

    private PreparedStatement getInsertStatement() throws SQLException {
        return manager.getConnection().prepareStatement(
                "INSERT INTO public.\"student\" " +
                        "VALUE  (firstName = ?, lastName = ?, secondName = ?, birthday = ?, groupId = ?)");
    }

    @Override
    public void insertOne(Student obj) throws StudentDAOException {
        PreparedStatement statement = null;
        try {
            statement = getInsertStatement();
            statement.setString(1, obj.getFirstName());
            statement.setString(2, obj.getFamilyName());
            statement.setString(3, obj.getSecondName());
            statement.setDate(4, Date.valueOf(obj.getBdate()));
            statement.setInt(5, obj.getGroup().getId());
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new StudentDAOException();
        }

    }

    @Override
    public void insertAll(List<Student> objColl) throws StudentDAOException {
        PreparedStatement statement = null;
        try {
            statement = getInsertStatement();
            for (Student obj : objColl) {
                statement.setString(1, obj.getFirstName());
                statement.setString(2, obj.getFamilyName());
                statement.setString(3, obj.getSecondName());
                statement.setDate(4, Date.valueOf(obj.getBdate()));
                statement.setInt(5, obj.getGroup().getId());
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new StudentDAOException();
        }
    }
}

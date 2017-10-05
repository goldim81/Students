package com.Students.DB.DAO;

import com.Students.DB.ConnectionManagerPostgreSQL;
import com.Students.DB.IConnectionManager;
import com.Students.Object.Lesson;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class LessonDAO implements IDAO<Lesson> {

    public static class LessonDAOException extends Exception {

    }

    private static IConnectionManager manager;

    static {
        manager = ConnectionManagerPostgreSQL.getInstance();
    }
    
    @Override
    public List<Lesson> getAll() throws LessonDAOException {
        List<Lesson> lessonList = new ArrayList<>();
        Statement statement = null;
        try {
            statement = manager.getConnection().createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM public.\"Lesson\"");
            while (resultSet.next()) {
                Lesson lesson = new Lesson(resultSet.getShort("id"), resultSet.getString("topic"), resultSet.getDate("DateTime").toLocalDate(),
                        resultSet.getInt("class_room"), resultSet.getString("teacher"));

                lessonList.add(lesson);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new LessonDAOException();
        }
        return lessonList;
    }

    @Override
    public Lesson getByID(int id) throws LessonDAOException {
        PreparedStatement statement = null;
        try {
            statement = manager.getConnection().prepareStatement("SELECT * FROM public.\"Lesson\" WHERE id = ? ");
            statement.setInt(1, id);
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            return new Lesson(resultSet.getShort("id"), resultSet.getString("topic"), resultSet.getDate("DateTime").toLocalDate(),
                    resultSet.getInt("class_room"), resultSet.getString("teacher"));
        } catch (SQLException e) {
            e.printStackTrace();
            throw new LessonDAOException();
        }
    }

    private PreparedStatement getUpdateStatement() throws SQLException {
        return manager.getConnection().prepareStatement(
                "UPDATE public.\"Lesson\" " +
                        "SET  topic = ?, DateTime = ?, class_room = ?, teacher = ?" +
                        "WHERE id = ? ");
    }

    @Override
    public void update(Lesson obj) throws LessonDAOException {
        PreparedStatement statement = null;
        try {
            statement = getUpdateStatement();
            setField(statement, obj);
            statement.setInt(5, obj.getId());
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new LessonDAOException();
        }
    }

    @Override
    public void updateAll(List<Lesson> objList) throws LessonDAOException {
        PreparedStatement statement = null;
        try {
            statement = getUpdateStatement();
            for (Lesson lesson : objList) {
                setField(statement, lesson);
                statement.setInt(5, lesson.getId());
                statement.addBatch();

            }
            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new LessonDAOException();
        }
    }

    @Override
    public void deleteByID(int id) throws LessonDAOException {
        PreparedStatement statement = null;
        try {
            statement = manager.getConnection().prepareStatement(
                    "DELETE public.\"student\" WHERE id = ? ");
            statement.setInt(1, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new LessonDAOException();
        }
    }
    
    private PreparedStatement getInsertStatement() throws SQLException {
        return manager.getConnection().prepareStatement(
                "INSERT INTO public.\"Lesson\" " +
                        "VALUE  (topic = ?, DateTime = ?, class_room = ?, teacher = ?)");
    }

    @Override
    public void insertOne(Lesson obj) throws LessonDAOException {
        PreparedStatement statement = null;
        try {
            statement = getInsertStatement();
            setField(statement, obj);
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new LessonDAOException();
        }
    }

    @Override
    public void insertAll(List<Lesson> objList) throws LessonDAOException {
        PreparedStatement statement = null;
        try {
            statement = getInsertStatement();
            for (Lesson obj : objList) {
                setField(statement, obj);
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new LessonDAOException();
        }
    }

    private void setField(PreparedStatement statement, Lesson obj) throws SQLException {
        statement.setString(1, obj.getTopic());
        statement.setDate(2, Date.valueOf(obj.getDateTime()));
        statement.setInt(3, obj.getClassRoom());
        statement.setString(4, obj.getTeacher());
    }
}

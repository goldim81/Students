package com.Students.DB.DAO;

import com.Students.DB.ConnectionManagerPostgreSQL;
import com.Students.DB.IConnectionManager;
import com.Students.Object.Group;
import com.Students.Object.Student;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class GroupDAO implements IDAO<Group> {

    public static class GroupDAOException extends Exception {

    }

    private static IConnectionManager manager;

    static {
        manager = ConnectionManagerPostgreSQL.getInstance();
    }

    @Override
    public Set<Group> getAll() throws GroupDAOException {
        Set<Group> groupSet = new HashSet<>();
        Statement statement = null;
        try {
            statement = manager.getConnection().createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * " +
                    "FROM public.\"Group\" ");
            while (resultSet.next()) {
                Group group = new Group(resultSet.getShort("id"), resultSet.getString("name"));

                groupSet.add(group);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GroupDAOException();
        }
        return groupSet;
    }

    @Override
    public Group getByID(int id) throws GroupDAOException {
        PreparedStatement statement = null;
        try {
            statement = manager.getConnection().prepareStatement("SELECT * FROM public.\"Group\" WHERE id = ? ");
            statement.setInt(1, id);
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            return new Group(resultSet.getShort("id"), resultSet.getString("name"));
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GroupDAOException();
        }
    }

    private PreparedStatement getUpdateStatement() throws SQLException {
        return manager.getConnection().prepareStatement(
                "UPDATE public.\"Group\" " +
                        "SET  name = ?" +
                        "WHERE id = ? ");
    }

    @Override
    public void update(Group obj) throws GroupDAOException {
        PreparedStatement statement = null;
        try {
            statement = getUpdateStatement();
            statement.setString(1, obj.getGroupName());
            statement.setInt(2, obj.getId());
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GroupDAOException();
        }

    }

    @Override
    public void updateAll(List<Group> objSet) throws GroupDAOException {
        PreparedStatement statement = null;
        try {
            statement = getUpdateStatement();
            for (Group group : objSet) {
                statement.setString(1, group.getGroupName());
                statement.setInt(2, group.getId());
                statement.addBatch();

            }
            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GroupDAOException();
        }

    }

    @Override
    public void deleteByID(int id) throws GroupDAOException {
        PreparedStatement statement = null;
        try {
            statement = manager.getConnection().prepareStatement(
                    "DELETE public.\"Group\" WHERE id = ? ");
            statement.setInt(1, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GroupDAOException();
        }
    }

    private PreparedStatement getInsertStatement() throws SQLException {
        return manager.getConnection().prepareStatement(
                "INSERT INTO public.\"Group\"  " +
                        "VALUE  (name = ?)");
    }

    @Override
    public void insertOne(Group obj) throws GroupDAOException {
        PreparedStatement statement = null;
        try {
            statement = getInsertStatement();
            statement.setString(1, obj.getGroupName());
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GroupDAOException();
        }


    }

    @Override
    public void insertAll(List<Group> objColl) throws GroupDAOException {
        PreparedStatement statement = null;
        try {
            statement = getInsertStatement();
            for (Group obj : objColl) {
                statement.setString(1, obj.getGroupName());
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GroupDAOException();
        }
    }
}

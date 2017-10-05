package com.Students;

import com.Students.DB.DAO.StudentDAO;
import com.Students.Object.Student;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        try {
            List<Student> studentList = StudentDAO.getAll();
            for (Student student : studentList) {
                System.out.println(student);
            }
        } catch (StudentDAO.StudentDAOException e) {
            e.printStackTrace();
        }
    }
}

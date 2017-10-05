//package com.Students.Object;
//
//import org.junit.jupiter.api.Test;
//
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.sql.Date;
//import java.util.HashSet;
//import java.util.Set;
//
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//class MySerializationActionTest {
//    @Test
//    void readJornal() {
//        ObjectInputStream mockStream = mock(ObjectInputStream.class);
//        Student s01 = new Student("Иван", "Иванович", "Иванов", Date.parse("1981/03/01"), 17, (short)1);
//        Lesson lesson01 = new Lesson("Тема1", Date.parse("2017/09/21"), (short)1, "Артем");
//        Group group01 = new Group(1, "Группа1");
//        group01.add(s01);
//        Set<Group> groups = new HashSet<>();
//        groups.add(group01);
//        try {
//            when(mockStream.readUnshared()).thenReturn(new Jornal(lesson01, group01.getStudents(), groups));
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//
//        Jornal restJornal = MySerializationAction.readJornal(mockStream);
//    }
//
//}
package com.Students.Object;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

public class Main {
    public static void main(String[] args) throws TransformerException, ParserConfigurationException, IllegalAccessException {
        try {
            DatabaseManager.showStudents();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
//        Student s01 = new Student("Иван", "Иванович", "Иванов", Date.parse("1981/03/01"), 17, (short)1);
//        Student s02 = new Student("Петр", "Петрович", "Петров", Date.parse("1981/02/01"), 20, (short)2);
//        Student s03 = new Student("Сидор", "Сидорович", "Сидоров", Date.parse("1981/04/01"), 61, (short)3);
//        Student s04 = new Student("Сергей", "Сергеевич", "Сергеев", Date.parse("1981/04/01"), 22, (short)4);
//        MySerializationAction.writeStudentToXML(s01);
//        try {
//
//            File file = new File("Student.xml");
//            JAXBContext jaxbContext = JAXBContext.newInstance(Student.class);
//            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
//
//            // output pretty printed
//            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
//
//            jaxbMarshaller.marshal(s01, file);
//            jaxbMarshaller.marshal(s01, System.out);
//
//        } catch (JAXBException e) {
//            e.printStackTrace();
//        }

//        try {
//
//            File file = new File("Student.xml");
//            JAXBContext jaxbContext = JAXBContext.newInstance(Student.class);
//
//            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
//            Student student = (Student) jaxbUnmarshaller.unmarshal(file);
//            System.out.println(student);
//
//        } catch (JAXBException e) {
//            e.printStackTrace();
//        }
//

//        Group group01 = new Group(1, "Группа1");
//        group01.add(s01);
//        group01.add(s02);
//        group01.add(s03);
//
//
//        Group group02 = new Group (2, "Группа2");
//        group02.add(s01);
//        group02.add(s04);
//
//        Set<Group> groups = new HashSet<>();
//        groups.add(group01);
//        groups.add(group02);
//
//        //Проверка чтения и записи студентов
//        MySerializationAction.serializeStudents(group01.getStudents());
//        Set<Student> restoreStudents = MySerializationAction.readStudents();
//        for (Student st:restoreStudents) {
//            System.out.println(st);
//        }
//
//        //Проверка записи и чтения групп
//        MySerializationAction.serializeGroups(groups);
//        Set<Group> restoreGroups = MySerializationAction.readGroups();
//        if (restoreGroups!=null) {
//            for (Group gr : restoreGroups) {
//                gr.printGroup();
//            }
//        }else System.out.println("Ошибка восстановления списка групп");
//
//        //Проверка записи / Чтения журнала уроков
//        Lesson lesson01 = new Lesson("Тема1", Date.parse("2017/09/21"), (short)1, "Артем");
//
//        Jornal jornal1 = new Jornal(lesson01, group02.getStudents(), groups);
//
//        MySerializationAction.serializeJornal(jornal1);
//        MySerializationAction.readJornal().printJornal();

    }
}

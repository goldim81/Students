package com.Students.Object;

import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashSet;
import java.util.Set;

public class MySerializationAction {

    static void serializeGroup (Group group){
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("group.txt"))) {
            oos.writeObject(group);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void serializeGroups (Set<Group> groups){
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("groups.txt"))) {
            oos.writeInt(groups.size());
            for (Group gr:groups) {
                oos.writeObject(gr);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static Group readGroup () {
        File file = new File("group.txt");
        if (file.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                return (Group) ois.readObject();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        } else System.out.println("Файл группы отсутствует.");
        return null;
    }



    static Set<Group> readGroups () {
        File file = new File("groups.txt");
        Set<Group> groups = new HashSet<>();
        if (file.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                int size = ois.readInt();
                for (int i = 0; i < size; i++) {
                    groups.add((Group) ois.readObject());
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return groups;
        } else System.out.println("Файл группы отсутствует.");
        return null;
    }

    static void serializeJornal (Jornal jornal){
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("jornal.txt"))) {
            oos.writeObject(jornal);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static Jornal readJornal(ObjectInputStream ois) {
        Jornal jornal = null;
        try {
            jornal = (Jornal) ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return jornal;
    }

    static Jornal readJornal () {
        File file = new File("jornal.txt");
        if (file.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                return (Jornal) ois.readObject();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        } else System.out.println("Файл журнала отсутствует.");
        return null;
    }

    static void serializeStudents (Set<Student> students){
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("students.txt"))) {
            oos.writeInt(students.size());
            for (Student st:students) {
                oos.writeObject(st);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static Set<Student> readStudents () {
        File file = new File("students.txt");
        Set<Student> students = new HashSet<>();
        if (file.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                int size = ois.readInt();
                for (int i = 0; i < size; i++) {
                    students.add((Student) ois.readObject());
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return students;
        } else System.out.println("Файл группы отсутствует.");
        return null;
    }

    static void writeStudentToXML(Student student) throws ParserConfigurationException, TransformerException, IllegalAccessException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = dbf.newDocumentBuilder();
        DOMImplementation implementation = builder.getDOMImplementation();
        Document document = implementation.createDocument(null, null, null);
        Element elementStudent = document.createElement("object");
        elementStudent.setAttribute("type","Student");
        for (Field field:student.getClass().getDeclaredFields()){
            Element elementField = document.createElement("field");
            String fieldName = field.getName();
            field.setAccessible(true);
            String value = field.get(student).toString();
            elementField.setAttribute("type", field.getType().getName());
            elementField.setAttribute("id", fieldName);
            elementField.setAttribute("value", value);
            elementStudent.appendChild(elementField);
        }

        for (Method method : student.getClass().getDeclaredMethods()) {
            Element elementMethods = document.createElement("method");
            elementMethods.setAttribute("id", method.getName());
            elementMethods.setAttribute("return", method.getReturnType().getName());
            for (Parameter parameter : method.getParameters()) {
                Element paramElement = document.createElement("arg");
                paramElement.setAttribute("id", parameter.getName());
                paramElement.setAttribute("type", parameter.getType().getName());
                elementMethods.appendChild(paramElement);
            }
            elementStudent.appendChild(elementMethods);
        }


        document.appendChild(elementStudent);
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        Result output = new StreamResult(new File("Student.xml"));
        Source source = new DOMSource(document);
        transformer.transform(source, output);

    }

    public Student readStudentFromXML(){
        return null;
    }

}

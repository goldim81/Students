package com.Students.Object;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@XmlRootElement
public class Group implements Serializable {
    private final int id;
    private final String groupName;
    private Set<Student> students = new HashSet<>();

    public Group(int id, String gName) {
        this.id = id;
        groupName = gName;
    }

    public void add (Student student) {
        students.add(student);
        student.setGroup(this);
    }

    public void remove (Student student) {
        students.remove(students);
        student.setGroup(null);
    }

    public int getId() {
        return id;
    }

    public String getGroupName() {
        return groupName;
    }

    public Set<Student> getStudents() {
        return students;
    }

    public void setStudents(Set<Student> students) {
        this.students = students;
        for (Student student : students) {
            student.setGroup(this);
        }
    }

    public void printGroup () {
        System.out.println(this);
        for (Student student: students) {
            String fio = student.getFamilyName() + " " + student.getFirstName() + " " + student.getSecondName();
            System.out.println(fio);
        }
        System.out.println("Всего учеников в группе: " + students.size());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Group)) return false;

        Group group = (Group) o;

        if (id != group.id) return false;
        return groupName.equals(group.groupName);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + groupName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Группа " +
                "№" + id +
                ", " + groupName;
    }
}

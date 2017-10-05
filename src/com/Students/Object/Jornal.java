package com.Students.Object;

import com.Students.Object.Student;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class Jornal implements Serializable{
    private Lesson lesson;
    private Set<Student> presentStudents = new HashSet<>();
    private Group group;

    public Jornal(Lesson lesson, Set<Student> presentStudents, Group group) {
        this.lesson = lesson;
        this.presentStudents = presentStudents;
        this.group = group;
    }

    public Lesson getLesson() {
        return lesson;
    }

    public void setLesson(Lesson lesson) {
        this.lesson = lesson;
    }

    public Set<Student> getPresentStudents() {
        return presentStudents;
    }

    public void setPresentStudents(Set<Student> presentStudents) {
        this.presentStudents = presentStudents;
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public void printJornal () {

        //Объеденяем список стундентов в группах с исключением дубликатов
        Group group = getGroup();
        Set<Student> lStud1 = group.getStudents();
        Set<Student> allStudent = new HashSet<>();
        allStudent.addAll(lStud1);

        //Вывод на консоль
        Set<Student> presSt = getPresentStudents();
        System.out.println(getLesson());
        for (Student st:allStudent) {
            String  fio = st.getFamilyName() + " " + st.getFirstName() + " " + st.getSecondName();
            String topic = getLesson().getTopic();
            String dateLesson = getLesson().getDateTime().toString();
            String attended = presSt.contains(st) ? "Был" : "Нет";
            System.out.println(fio + " | " + topic + " | " + dateLesson + " | " + attended);
        }
        System.out.println("Всего учеников: " + allStudent.size() + " Присутсвовало на уроке: " + presSt.size());


    }
}

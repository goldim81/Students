package com.Students.Object;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.time.LocalDate;

@XmlRootElement
public class Student implements Serializable {
    private String firstName;
    private String secondName;
    private String familyName;
    @XmlElement
    private LocalDate bdate;
    private Group group;
    private int id;

    public Student() {
    }

    public Student(int id, String firstName, String secondName, String familyName, LocalDate bdate) {
        this.firstName = firstName;
        this.secondName = secondName;
        this.familyName = familyName;
        this.bdate = bdate;
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    @XmlElement
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getSecondName() {
        return secondName;
    }

    @XmlElement
    public void setSecondName(String secondName) {
        this.secondName = secondName;
    }

    public String getFamilyName() {
        return familyName;
    }

    @XmlElement
    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public LocalDate getBdate() {
        return bdate;
    }

    public int getId() {
        return id;
    }

    @XmlElement
    public void setId (int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Student{" +
                firstName + ' ' +
                secondName + ' ' +
                familyName +
                '}';
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
        group.add(this);
    }

    public void setBdate(LocalDate bdate) {
        this.bdate = bdate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Student)) return false;

        Student student = (Student) o;

        if (getId() != student.getId()) return false;
        if (getFirstName() != null ? !getFirstName().equals(student.getFirstName()) : student.getFirstName() != null)
            return false;
        if (getSecondName() != null ? !getSecondName().equals(student.getSecondName()) : student.getSecondName() != null)
            return false;
        if (getFamilyName() != null ? !getFamilyName().equals(student.getFamilyName()) : student.getFamilyName() != null)
            return false;
        return getBdate() != null ? getBdate().equals(student.getBdate()) : student.getBdate() == null;
    }

    @Override
    public int hashCode() {
        int result = getFirstName() != null ? getFirstName().hashCode() : 0;
        result = 31 * result + (getSecondName() != null ? getSecondName().hashCode() : 0);
        result = 31 * result + (getFamilyName() != null ? getFamilyName().hashCode() : 0);
        result = 31 * result + (getBdate() != null ? getBdate().hashCode() : 0);
        result = 31 * result + getId();
        return result;
    }
}

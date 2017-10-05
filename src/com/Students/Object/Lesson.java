package com.Students.Object;

import java.io.Serializable;
import java.time.LocalDate;

public class Lesson implements Serializable{
    private final int id;
    private final String topic;
    private final LocalDate dateTime;
    private final int classRoom;
    private final String teacher;

    public Lesson(int id, String topic, LocalDate dateTime, int classRoom, String teacher) {
        this.id = id;
        this.topic = topic;
        this.dateTime = dateTime;
        this.classRoom = classRoom;
        this.teacher = teacher;
    }

    public int getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public LocalDate getDateTime() {
        return dateTime;
    }

    public int getClassRoom() {
        return classRoom;
    }

    public String getTeacher() {
        return teacher;
    }

    @Override
    public String toString() {
        return "Урок на тему: " + topic;
    }
}

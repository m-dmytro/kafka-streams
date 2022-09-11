package com.hw.kafkastreams.model;

import java.util.Objects;

public class Person {

    private String name;
    private String company;
    private String position;
    private Integer experience;

    public Person() {}

    public Person(String name, String company, String position, Integer experience) {
        this.name = name;
        this.company = company;
        this.position = position;
        this.experience = experience;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public Integer getExperience() {
        return experience;
    }

    public void setExperience(Integer experience) {
        this.experience = experience;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Objects.equals(name, person.name) && Objects.equals(company, person.company) && Objects.equals(position, person.position) && Objects.equals(experience, person.experience);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, company, position, experience);
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", company='" + company + '\'' +
                ", position='" + position + '\'' +
                ", experience=" + experience +
                '}';
    }

    public boolean isEmptyObj() {
        return name == null && company == null && position == null && experience == null;
    }
}

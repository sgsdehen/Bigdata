package com.lancer.java.model;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@ToString
public class Person {
    private String name;
    private String gender;
    private int age;
}

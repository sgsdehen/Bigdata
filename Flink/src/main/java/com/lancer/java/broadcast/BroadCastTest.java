package com.lancer.java.broadcast;

public class BroadCastTest {

    public BroadCastTest(){
        System.out.println("A");
    }

    {
        System.out.println("C");
    }

    static {
        System.out.println("B");
    }

    public static void main(String[] args) {
         BroadCastTest brodCastTest = new BroadCastTest();
    }
}

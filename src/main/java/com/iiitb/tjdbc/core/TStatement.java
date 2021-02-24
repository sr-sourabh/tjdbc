package com.iiitb.tjdbc.core;

public class TStatement {
    public void processQuery(String query) {
        System.out.println("Intercepted");
        System.out.println(query);
    }
}

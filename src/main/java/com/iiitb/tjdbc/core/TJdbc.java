package com.iiitb.tjdbc.core;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TJdbc {
    public static Statement createStatement(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        return (Statement) Proxy.newProxyInstance(statement.getClass().getClassLoader(),
                new Class[]{Statement.class},
                new DynamicInvocationHandler(statement));
    }
}

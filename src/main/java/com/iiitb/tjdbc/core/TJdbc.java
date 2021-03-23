package com.iiitb.tjdbc.core;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class TJdbc {

    private final Set<String> keywords = new HashSet<>();
    public static final String LAST = "last";
    public static final String FIRST = "first";
    public static final String TEMPORALIZE = "temporalize";

    public TJdbc() {
        populateKeyWords();
    }

    private void populateKeyWords() {
        keywords.add(LAST);
        keywords.add(FIRST);
        keywords.add(TEMPORALIZE);
    }

    public static Statement createStatement(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        return (Statement) Proxy.newProxyInstance(statement.getClass().getClassLoader(),
                new Class[]{Statement.class},
                new DynamicInvocationHandler(statement));
    }

    public Set<String> getKeywords() {
        return keywords;
    }
}

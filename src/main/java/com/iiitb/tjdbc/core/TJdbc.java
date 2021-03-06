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
    public static final String TINSERT = "tinsert";
    public static final String TEMPORALIZE = "temporalize";
    public static final String TUPDATE = "tupdate";
    public static final String FROM = "from";
    public static final String SET = "set";
    public static final String TSELECT = "tselect";
    public static final String PREVIOUS = "previous";
    public static final String NEXT = "next";
    public static final String TJOIN = "tjoin";
    public static final String COALESCE = "coalesce";
    public static final String EVOLUTIONFROM = "evolutionfrom";
    public static final String EVOLUTIONFROMANDTO = "evolutionfromandto";
    public static final String EVOLUTION_HISTORY = "evolution_history";
    public static final String TDELETE = "tdelete";
    public static final String DIFFERENCE = "difference";

    public TJdbc() {
        populateKeyWords();
    }

    private void populateKeyWords() {
        keywords.add(LAST);
        keywords.add(FIRST);
        keywords.add(TEMPORALIZE);
        keywords.add(TINSERT);
        keywords.add(TUPDATE);
        keywords.add(FROM);
        keywords.add(SET);
        keywords.add(TSELECT);
        keywords.add(PREVIOUS);
        keywords.add(NEXT);
        keywords.add(TJOIN);
        keywords.add(EVOLUTION_HISTORY);
        keywords.add(TDELETE);
        keywords.add(COALESCE);
        keywords.add(EVOLUTIONFROM);
        keywords.add(EVOLUTIONFROMANDTO);
        keywords.add(DIFFERENCE);
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

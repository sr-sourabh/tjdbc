package com.iiitb.tjdbc.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

public class TStatement {

    private TJdbc tJdbc;

    public String processQuery(String query, Object o) throws SQLException {
        System.out.println("Recieved query for processing: " + query);

        Statement statement = (Statement) o;
        //contains tokens in the original query string
        List<String> tokens = new ArrayList<>();
        //key = keyword, value = index of keyword in tokens list
        Map<String, Integer> keywordPositionMap = new HashMap<>();
        tJdbc = new TJdbc();
        tokens = tokenize(query, tokens, keywordPositionMap);

        if (keywordPositionMap.containsKey(TJdbc.TEMPORALIZE)) {
            query = handleTemporalize(tokens);
            System.out.println(query);
        } else if (keywordPositionMap.containsKey(TJdbc.FIRST)) {
            query = handlefirst(keywordPositionMap, tokens);
        } else if (keywordPositionMap.containsKey(TJdbc.LAST)) {
            query = handlelast(keywordPositionMap, tokens);
        } else if (keywordPositionMap.containsKey(TJdbc.TINSERT)) {
            query = handleInsert(query, tokens);
        } else if (keywordPositionMap.containsKey(TJdbc.TUPDATE)) {
            query = handleTUpdate(query, tokens, keywordPositionMap, statement);
        }

        return query;
    }

    private String handleTUpdate(String query, List<String> tokens, Map<String, Integer> keywordPositionMap, Statement statement) throws SQLException {
        String tableName = getTableName(keywordPositionMap, tokens);
        Map<String, Integer> columnNameIndexMap = getColumnNameIndexMap(tableName, statement);
        return query;
    }

    private Map<String, Integer> getColumnNameIndexMap(String tableName, Statement statement) throws SQLException {
        ResultSet resultSet = statement.executeQuery("desc " + tableName + ";");
        int i = 0;
        Map<String, Integer> columnNameIndexMap = new HashMap<>();
        while (resultSet.next()) {
            columnNameIndexMap.put(resultSet.getString(1), i++);
        }
        return columnNameIndexMap;
    }

    private String getTableName(Map<String, Integer> keywordPositionMap, List<String> tokens) {
        return tokens.get(keywordPositionMap.getOrDefault(TJdbc.FROM, 0) + 1);
    }

    private List<String> tokenize(String query, List<String> tokens, Map<String, Integer> keywordPositionMap) {
        if (Objects.nonNull(query)) {
            tokens.addAll(Arrays.asList(query.split("\\s+")));
            tokens = tokens.stream().map(String::toLowerCase).collect(Collectors.toList());
            for (int i = 0; i < tokens.size(); i++) {
                if (tJdbc.getKeywords().contains(tokens.get(i))) {
                    keywordPositionMap.put(tokens.get(i), i);
                }
            }
        }

        return tokens;
    }


    public String handleTemporalize(List<String> tokens) {
        String tableName = tokens.get(1);
        String tempTable = tableName + "_VT";
        String tempQuery = "CREATE TABLE " + tempTable +
                "(id integer primary key ,indx integer,updated_value varchar(10)," +
                "prev_value varchar(10),VST timestamp,VET timestamp,id_id int);";
        return tempQuery;
    }

    public String handleInsert(String currQuery, List<String> tokens) {
        // currQuery --> "Insert into Student values ('Mike',1,'active','3.4','CSE')"
        // finding current time stamp
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String lsst = timestamp.toString();
        //String lsst = "2011-01-01 00:00:02";
        String lset = "2037-01-19 03:14:07";
        currQuery = currQuery.substring(0, currQuery.length() - 1);
        String addTimestamp = ", '" + lsst + "', '" + lset + "');";
        currQuery = currQuery + addTimestamp;
        System.out.println(currQuery);
        return currQuery;
    }

    public String handlefirst(Map<String, Integer> keywordPositionMap, List<String> tokens) {
        int i = 0;
        String query = "";
        String where = "";
        for (String s : tokens) {
            if (s.equals("first"))
                continue;
            if (s.equals("where"))
                i = 1;
            if (i == 1)
                where = where + s + " ";
            else
                query = query + s + " ";
        }
        if ("".equals(where))
            query = query + "d join student_VT a on d.id = a.id_id  " +
                    "where (d.id,a.VST) in(select d.id,min(a.VST) from student d " +
                    "join student_VT a on d.id = a.id_id group by d.id);";
        else
            query = query + "d join student_VT a on d.id = a.id_id " + where + "order by a.VST limit 1;";
//        select * from student d join student_VT a on d.id = a.id_id where name="ayush" order by a.VST limit 1;
//      select * from student d join student_VT a on d.id = a.id_id  where (d.id,a.VST) in(select d.id,min(a.VST) from student d join student_VT a on d.id = a.id_id group by d.id);

        return query;
    }

    public String handlelast(Map<String, Integer> keywordPositionMap, List<String> tokens) {
        String query = "";
        for (String s : tokens) {
            if (s.equals("last"))
                continue;
            query = query + s + " ";
        }
        query = query + ";";

        return query;
    }

}

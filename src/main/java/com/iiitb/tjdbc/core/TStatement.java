package com.iiitb.tjdbc.core;

import java.util.*;
import java.util.stream.Collectors;
import java.sql.Timestamp;
public class TStatement {

    private TJdbc tJdbc;

    public String processQuery(String query) {
        System.out.println("Recieved query for processing: " + query);

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
        }else if (keywordPositionMap.containsKey(TJdbc.INSERT)) {
            System.out.println("hey" + query);
           query = handleInsert(query, tokens);
        }

        return query;
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


    public String handleTemporalize(List<String> tokens){
        String tableName = tokens.get(1);
        String tempTable = tableName + "_VT";
        String tempQuery = "CREATE TABLE " + tempTable + "(id integer primary key ,indx integer,updated_value varchar(10),prev_value varchar(10),VST timestamp,VET timestamp,id_student int);";
        return tempQuery;
    }

    public String handleInsert(String currQuery, List<String> tokens) {
        // currQuery --> "Insert into Student values ('Mike',1,'active','3.4','CSE')"
        // finding current time stamp
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String lsst = timestamp.toString();
        //String lsst = "2011-01-01 00:00:02";
        String lset = "2037-01-19 03:14:07";
        currQuery = currQuery.substring(0,currQuery.length()-1);
        String addTimestamp =  ", '" + lsst + "', '" + lset + "');";
        currQuery = currQuery + addTimestamp;
        System.out.println(currQuery);
        return currQuery;
    }
    public void handlefirst(Map<String, Integer> keywordPositionMap, List<String> tokens) {
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
        if (where == "")
            query = query + "d join student_VT a on d.id = a.id_student  where (d.id,a.VST) in(select d.id,min(a.VST) from student d join student_VT a on d.id = a.id_student group by d.id);";
        else
            query = query + "d join student_VT a on d.id = a.id_student " + where + "order by a.VST limit 1;";
//        select * from student d join student_VT a on d.id = a.id_student where name="ayush" order by a.VST limit 1;
//      select * from student d join student_VT a on d.id = a.id_student  where (d.id,a.VST) in(select d.id,min(a.VST) from student d join student_VT a on d.id = a.id_student group by d.id);

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

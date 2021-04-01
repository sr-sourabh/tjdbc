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
        query = query.toLowerCase();
        System.out.println("Recieved query for processing: " + query);

        Statement statement = (Statement) o;
        //contains tokens in the original query string
        List<String> tokens = new ArrayList<>();
        //key = keyword, value = index of keyword in tokens list
        Map<String, Integer> keywordPositionMap = new HashMap<>();
        tJdbc = new TJdbc();
        tokens = tokenize(query, tokens, keywordPositionMap);

        if (keywordPositionMap.containsKey(TJdbc.TEMPORALIZE)) {
            query = handleTemporalize(tokens, statement);
            System.out.println(query);
        } else if (keywordPositionMap.containsKey(TJdbc.FIRST)) {
            query = handlefirst(keywordPositionMap, tokens, statement, query);
        } else if (keywordPositionMap.containsKey(TJdbc.LAST)) {
            query = handlelast(keywordPositionMap, tokens);
        } else if (keywordPositionMap.containsKey(TJdbc.TINSERT)) {
            query = handleInsert(query, tokens);
        } else if (keywordPositionMap.containsKey(TJdbc.TUPDATE)) {
            query = handleTUpdate(query, tokens, keywordPositionMap, statement);
        } else if (keywordPositionMap.containsKey(TJdbc.TSELECT)) {
            query = handleTSelect(query, tokens, keywordPositionMap, statement);
        }

        return query;
    }

    private String handleTUpdate(String query, List<String> tokens, Map<String, Integer> keywordPositionMap, Statement statement) throws SQLException {
        String tableName = getToken(keywordPositionMap, tokens, TJdbc.TUPDATE, 1);
        Map<String, Integer> columnNameIndexMap = getColumnNameIndexMap(tableName, statement);
        query = query.replace(TJdbc.TUPDATE, "update");
        String pureUpdateQuery = query;

        String tableVt = tableName + "_vt";
        int indx = columnNameIndexMap.get(getToken(keywordPositionMap, tokens, TJdbc.SET, 1));

        //get prev value from original table and corresponding table id
        ResultSet rsForPrevValues = getPrevValueRs(statement, tokens, keywordPositionMap, query, tableName);
        String prevValue = rsForPrevValues.getString(1);
        String idId = rsForPrevValues.getString(2);

        //update end date in vt table
        String selectLastRecordTableVtQuery = "select id from " + tableVt + " where indx = " + indx +
                " and vet is NULL and id_id = " + idId + " limit 1 ; ";
        ResultSet resultSet = statement.executeQuery(selectLastRecordTableVtQuery);
        Integer idToUpdateDate = null;
        if (resultSet.next()) {
            idToUpdateDate = resultSet.getInt(1);
        }
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String vet = timestamp.toString();
        if (Objects.nonNull(idToUpdateDate)) {
            String updateDateQuery = "update " + tableVt + " set vet = '" + vet + "' where id = " + idToUpdateDate;
            statement.executeUpdate(updateDateQuery);
        }

        String updatedValue = getUpdateValue(tokens);

        //insert new record in vt table to reflect update
        String insertHistoryRecordTableVtQuery = "insert into " + tableVt +
                " (indx, updated_value, prev_value, vst, id_id) values (" +
                indx + ", '" + updatedValue + "', '" + prevValue + "', '" + vet + "', " + Integer.parseInt(idId) + ")";
        statement.executeUpdate(insertHistoryRecordTableVtQuery);


        return pureUpdateQuery;
    }

    private String getUpdateValue(List<String> tokens) {
        int i = 0;
        while (i < tokens.size() && !tokens.get(i).equals("=")) i++;
        return tokens.get(i + 1);
    }

    private ResultSet getPrevValueRs(Statement statement, List<String> tokens, Map<String, Integer> keywordPositionMap, String query, String tableName) throws SQLException {
        String columnName = getToken(keywordPositionMap, tokens, TJdbc.SET, 1);
        String selectPrevValueQuery = "select " + columnName + ", id " + " from " + tableName;
        int i = 0;
        while (i < tokens.size() && !tokens.get(i).equals("where")) {
            i++;
        }
        while (i < tokens.size()) {
            selectPrevValueQuery += " " + tokens.get(i);
            i++;
        }
        ResultSet resultSet = statement.executeQuery(selectPrevValueQuery);
        resultSet.next();
        return resultSet;
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

    private String getToken(Map<String, Integer> keywordPositionMap, List<String> tokens, String keyword, int offset) {
        return tokens.get(keywordPositionMap.getOrDefault(keyword, 0) + offset);
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

    public String handleTSelect(String query, List<String> tokens, Map<String, Integer> keywordPositionMap, Statement statement) throws SQLException {

        // "tselect gpa from student where id=1 and date='2019-01-12' "

        String tableName = getToken(keywordPositionMap, tokens, TJdbc.TSELECT, 3);
        Map<String, Integer> columnNameIndexMap = getColumnNameIndexMap(tableName, statement);
        int indx = columnNameIndexMap.get(tokens.get(1));
        //
        String tableVT = tableName + "_vt";
        int idIndx = 0; // stores the index where the id resides
        int dateIndx = 0; // stores the index where the date resides
        int n = tokens.size();
        for (int i = 0; i < tokens.size(); i++) {
            if (tokens.get(i).equals("id")) {
                idIndx = i + 2;
            }
            if (tokens.get(i).equals("date")) {
                dateIndx = i + 2;
            }
        }
        String id_id = tokens.get(idIndx);
        String time = tokens.get(dateIndx);
        String tempQuery = "select * from " + tableVT + " where indx = " + indx + " and id_id = " + id_id + " and vst<= " +
                time + " and vet > " + time + " ;";
        query = tempQuery;
        return query;
    }


    public String handleTemporalize(List<String> tokens, Statement statement) throws SQLException {
        String tableName = tokens.get(1);
        String tempTable = tableName + "_vt";
        //check if vt table already exists
        ResultSet resultSet = statement.executeQuery("show tables like '" + tempTable + "'");

        String tempQuery = "desc " + tempTable;
        if (!resultSet.next()) {
            tempQuery = "CREATE TABLE " + tempTable +
                    "(id integer AUTO_INCREMENT primary key ,indx integer,updated_value varchar(10)," +
                    "prev_value varchar(10),VST timestamp,VET timestamp,id_id int);";
        }

        return tempQuery;
    }

    public String handleInsert(String currQuery, List<String> tokens) {
        // currQuery --> "Insert into Student values ('Mike',1,'active','3.4','CSE')"
        // finding current time stamp
        currQuery = currQuery.replace(TJdbc.TINSERT, "insert");
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

    public String handlefirst(Map<String, Integer> keywordPositionMap, List<String> tokens, Statement statement, String query) throws SQLException {
        String tableName = getToken(keywordPositionMap, tokens, TJdbc.FROM, 1);
        tokens.remove(keywordPositionMap.getOrDefault(TJdbc.FIRST, 0).intValue());
        String tableVt = tableName + "_vt";

        int i = 0;
        String newQuery = "";
        while (i < tokens.size() && !tokens.get(i).equals("where")) {
            if (tokens.get(i).equals("from")) {
                newQuery += ", prev_value ";
            }
            newQuery += tokens.get(i++) + " ";
        }
        //where is present
        if (i < tokens.size()) {
            newQuery += "d join " + tableVt + " a on d.id = a.id_id ";
            while (i < tokens.size()) {
                newQuery += tokens.get(i++) + " ";
            }
            newQuery += " and (d.id,a.VST) in(select d.id,min(a.VST) from " + tableName +
                    " d join " + tableVt + " a on d.id = a.id_id group by d.id)";
        }
        //where is not present
        else {
            newQuery += "d join " + tableVt + " a on d.id = a.id_id ";
            newQuery += " where (d.id,a.VST) in(select d.id,min(a.VST) from " + tableName +
                    " d join " + tableVt + " a on d.id = a.id_id group by d.id)";
        }

        //select * from student d join student_vt a on d.id = a.id_id  where (d.id,a.VST) in
        // (select d.id,min(a.VST) from student d join student_vt a on d.id = a.id_id group by d.id);

        return newQuery;
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

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
        } else if (keywordPositionMap.containsKey(TJdbc.PREVIOUS)) {
            query = handlePrevious(keywordPositionMap, tokens, statement);
        } else if (keywordPositionMap.containsKey(TJdbc.NEXT)) {
            query = handleNext(keywordPositionMap, tokens, statement);
        } else if (keywordPositionMap.containsKey(TJdbc.TJOIN)) {
            query = handleTjoin(keywordPositionMap, tokens, statement);
        } else if (keywordPositionMap.containsKey(TJdbc.TSELECT)) {
            query = handleTSelect(query, tokens, keywordPositionMap, statement);
        } else if (keywordPositionMap.containsKey(TJdbc.COALESCE)) {
            query = handleCoalesce(tokens, statement);
        } else if (keywordPositionMap.containsKey(TJdbc.EVOLUTIONFROM)) {
            query = handleEvolutionFrom(keywordPositionMap, tokens, statement);
        } else if (keywordPositionMap.containsKey(TJdbc.EVOLUTIONFROMANDTO)) {
            query = handleEvolutionFromAndTo(keywordPositionMap, tokens, statement);
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
        Map<String, Integer> columnNameIndexMap = getColumnNameIndexMap(tableName, statement);
        int indx = columnNameIndexMap.get(getToken(keywordPositionMap, tokens, TJdbc.FIRST, 1));
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
            newQuery += " and (d.id,a.VST) in(select d.id, min(a.VST) from " + tableName +
                    " d join " + tableVt + " a on d.id = a.id_id and indx = " + indx + " group by d.id) and indx = " + indx;
        }
        //where is not present
        else {
            newQuery += "d join " + tableVt + " a on d.id = a.id_id ";
            newQuery += " where (d.id,a.VST) in(select d.id, min(a.VST) from " + tableName +
                    " d join " + tableVt + " a on d.id = a.id_id and indx = " + indx + " group by d.id) and indx = " + indx;
        }

        //select d.id, gpa , prev_value from student d join student_vt a on d.id = a.id_id
        // where (d.id,a.VST) in(select d.id,min(a.VST) from student d join student_vt a
        // on d.id = a.id_id and indx = 3 group by d.id, indx) and indx = 3;

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

    public String handlePrevious(Map<String, Integer> keywordPositionMap, List<String> tokens, Statement statement) throws SQLException {
        String query = "";
        String tableName = tokens.get(5);
        ;
        String tableVt = tableName + "_vt";
        String value = tokens.get(2);

        Map<String, Integer> columnNameIndexMap = getColumnNameIndexMap(tokens.get(5), statement);
        int indx = columnNameIndexMap.get(tokens.get(3));
        String id = tokens.get(7);

        id = id.substring(0, id.length() - 1);

        query = tokens.get(0) + " id_id, prev_value, vst, vet from " + tableVt + " where id_" + id +
                " and indx=" + indx + " and updated_value = " + "'" + value + "'" + " ;";

//        select previous Civil major from student where id=1;
//        select id_id, prev_value,VST,VET from student_VT where id_id=1 and indx=4 and updated_value = "Civil";
        return query;
    }

    public String handleNext(Map<String, Integer> keywordPositionMap, List<String> tokens, Statement statement) throws SQLException {
        String query = "";
        String tableName = tokens.get(5);
        ;
        String tableVt = tableName + "_vt";

        String value = tokens.get(2);

        Map<String, Integer> columnNameIndexMap = getColumnNameIndexMap(tokens.get(5), statement);
        int indx = columnNameIndexMap.get(tokens.get(3));
        String id = tokens.get(7);

        id = id.substring(0, id.length() - 1);

        query = tokens.get(0) + " id_id, updated_value, vst, vet from " + tableVt + " where id_" + id +
                " and indx=" + indx + " and prev_value = " + "'" + value + "'" + " ;";

//        select next CSE major from student where id=1;
//        select id_id, updated_value,VST,VET from student_VT where id_id=1 and indx=4 and prev_value = "CSE";
        return query;
    }

    private String handleTjoin(Map<String, Integer> keywordPositionMap, List<String> tokens, Statement statement) {
        String query = "";
        String table1 = tokens.get(1);
        String table1_vt = table1 + "_vt";
        String e = tokens.get(2);
        String table2 = tokens.get(4);
        String table2_vt = table2 + "_vt";
        String m = tokens.get(5);
        String f1 = (tokens.get(7));
        String f = f1.substring(1);
        String l1 = (tokens.get(9));
        String l = l1.substring(1);

        String where = "";
        if (tokens.get(10).equals("where")) {
            where = "where " + m + ".id_id = " + tokens.get(13);
        }

        query = "select " + m + ".id_id as " + table2 + "_id," + m + ".updated_value as m_id, " + e + ".id_id as " + table1 + "_id," +
                " " + e + ".updated_value as " + table2 + "_id," +
                " (case when " + m + ".vst > " + e + ".vst then " + m + ".vst else " + e + ".vst end) as finalstartdate," +
                " (case when " + m + ".vet < " + e + ".vet then " + m + ".vet else " + e + ".vet end) as finalenddate" +
                " from " + table2_vt + " " + m + " join " + table1_vt + " " + e + " on " + e + ".updated_value = " + m + ".id_id" +
                " and ((" + m + ".vst between " + e + ".vst and " + e + ".vet) or (" + e + ".vst between " + m + ".vst and " + m + ".vet)) " + where + ";";

//        user query
//        tselect employee e tjoin department m on e.did = m.did ;
//        or
//        tselect employee e tjoin department m on e.d_id = m.d_id where m.d_id = 1;


//        modified query
//        select m.id_id as department_id,m.updated_value as m_id, e.id_id as employee_id, e.updated_value as department_id, (case when m.vst > e.vst then m.vst else e.vst end) as finalstartdate,
//        (case when m.vet < e.vet then m.vet else e.vet end) as finalenddate from department_vt m join employee_vt e on e.updated_value = m.id_id
//        and ((m.vst between e.vst and e.vet) or (e.vst between m.vst and m.vet)) ;

        return query;
    }

    private String handleCoalesce(List<String> tokens, Statement statement) throws SQLException {
        String table = tokens.get(1) + "_vt";
        Map<String, Integer> columnNameIndexMap = getColumnNameIndexMap(table, statement);
        String columnList = "";
        int mapSize = columnNameIndexMap.size();
        for (Map.Entry<String, Integer> entry : columnNameIndexMap.entrySet()) {
            if (entry.getValue() < mapSize - 2) {
                columnList += entry.getKey();
                columnList += ",";
            }
        }
        columnList = columnList.substring(0, columnList.length() - 1);

        String query = "select " + columnList + " ,min(stt) start_time, max(ett) end_time from " + table + " group by " + columnList;
        System.out.println(query);
        return query;
    }

    public String handleEvolutionFrom(Map<String, Integer> keywordPositionMap, List<String> tokens, Statement statement) throws SQLException {
        String query = "";
        String tableName = tokens.get(1);

        String tableVt = tableName + "_vt";

        String value1 = tokens.get(3);
        String where = "";

        Map<String, Integer> columnNameIndexMap = getColumnNameIndexMap(tokens.get(1), statement);
        int indx = columnNameIndexMap.get(tokens.get(2));

        if (tokens.get(4).equals("where")) {
            where = " and s.id_id = " + tokens.get(7) + "";
        }

        query = "with temp as (select vst,id_id from " + tableVt + " where indx = " + indx + "" +
                " and updated_value = " + value1 + ") select * from " + tableVt + " s" +
                " join temp t on s.id_id = t.id_id and s.vst >= t.vst where s.indx=" + indx + where + " ;";


//        User Query
//        EvolutionFrom student gpa 5.2 ;
//        EvolutionFrom student gpa 5.2 where id = 1 ;

// updated query
//        with temp as (select vst,id_id from student_vt where indx = 3 and updated_value = 5.2) select * from student_vt s join temp t on s.id_id = t.id_id and s.vst >= t.vst where s.indx=3 ;
        return query;
    }

    public String handleEvolutionFromAndTo(Map<String, Integer> keywordPositionMap, List<String> tokens, Statement statement) throws SQLException {
        String query = "";
        String tableName = tokens.get(1);

        String tableVt = tableName + "_vt";

        String value1 = tokens.get(3);
        String value2 = tokens.get(4);
        String where = "";

        Map<String, Integer> columnNameIndexMap = getColumnNameIndexMap(tokens.get(1), statement);
        int indx = columnNameIndexMap.get(tokens.get(2));

        if (tokens.get(5).equals("where")) {
            where = " and s.id_id = "+tokens.get(8)+"";
        }

        query = "with temp as (select vst,id_id from "+tableVt+" where indx = "+indx+" and updated_value = "+value1+") ," +
                "  temp2 as (select vst,id_id from "+tableVt+" where indx = "+indx+" and updated_value = "+value2+")" +
                "  select * from "+tableVt+" s join temp t on s.id_id = t.id_id and s.vst >= t.vst" +
                " join temp2 t2 on t2.id_id=s.id_id and s.vst<=t2.vst where s.indx="+indx+where+" ;";


//        User Query
//        EvolutionFromAndTo student gpa 5.2 2.9 ;
//        EvolutionFromAndTo student gpa 5.2 2.9 where id = 1 ;

// updated query
//        with temp as (select vst,id_id from student_vt where indx = 3 and updated_value = 5.2) ,  temp2 as (select vst,id_id from student_vt where indx = 3 and updated_value = 2.9)  select * from student_vt s join temp t on s.id_id = t.id_id and s.vst >= t.vst join temp2 t2 on t2.id_id=s.id_id and s.vst<=t2.vst where s.indx=3;
//        with temp as (select vst,id_id from student_vt where indx = 3 and updated_value = 5.2) ,  temp2 as (select vst,id_id from student_vt where indx = 3 and updated_value = 2.9;)  select * from student_vt s join temp t on s.id_id = t.id_id and s.vst >= t.vst join temp2 t2 on t2.id_id=s.id_id and s.vst<=t2.vst where s.indx=3;

        return query;
    }
}


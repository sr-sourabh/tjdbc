package com.iiitb.tjdbc;

import com.iiitb.tjdbc.core.TJdbc;

import java.sql.*;

import static com.iiitb.tjdbc.util.CommonUtils.getUrl;
import static com.iiitb.tjdbc.util.ConnectionDetails.*;

public class Application {

    public static void main(String[] args) {
        try {
            Class.forName(DRIVER);
            Connection connection = DriverManager.getConnection(getUrl(), USER, PASSWORD);
            Statement statement = TJdbc.createStatement(connection);

            //uncomment only the part you are working on
            //executeTemporalize(statement);
            //executeInsert(statement);
            //executeFirst(statement);
            //executeLast(statement);
            //executeTUpdate(statement);
            executeTSelectOnDate(statement);
            connection.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void executeTUpdate(Statement statement) throws SQLException {
        String query = "tupdate student set gpa = 8.2 where id = 1 ";
        statement.executeUpdate(query);
    }

    private static void executeInsert(Statement statement) throws SQLException {
        String query = "tinsert into student values (6,'Henry','active','3.75','CSE')";
        statement.executeUpdate(query);
    }

    private static void executeLast(Statement statement) throws SQLException {
        String query = "Select last salary from domain where name='ayush'";
        ResultSet resultset = statement.executeQuery(query);

        while (resultset.next()) {
            System.out.println(resultset.getString(1) + " " + resultset.getInt(2));
        }
    }

    private static void executeFirst(Statement statement) throws SQLException {
        String query = "Select first salary from domain where name='ayush'";
        ResultSet resultset = statement.executeQuery(query);

        while (resultset.next()) {
            System.out.println(resultset.getString(1) + " " + resultset.getInt(2));
        }
    }

    private static void executeTemporalize(Statement statement) throws SQLException {
        String query = "Temporalize ssss";
        statement.executeUpdate(query);
    }

    private static void executeTSelectOnDate(Statement statement) throws SQLException{
        String query = "tselect gpa from student where id = 1 and date = '2019-01-12' ";
        ResultSet resultset = statement.executeQuery(query);

        while (resultset.next()) {
            // can parse the results accordinly with user needs
            System.out.println("Current results are : " + resultset.getString(3) + " for student id " + resultset.getInt(7));
        }
    }
}
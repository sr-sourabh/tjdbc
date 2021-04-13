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
            //executeTSelectOnDate(statement);
            //executePrevious(statement);
            //executeNext(statement);
            executeTjoin(statement);

            connection.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void executeTUpdate(Statement statement) throws SQLException {
        String query = "tupdate student set gpa = 10.2 where id = 1 ";
        statement.executeUpdate(query);
    }

    private static void executeInsert(Statement statement) throws SQLException {
        String query = "tinsert into student values (6,'Henry','active','3.75','CSE')";
        statement.executeUpdate(query);
    }

    private static void executeLast(Statement statement) throws SQLException {
        String query = "Select last id, gpa from student";
        ResultSet resultset = statement.executeQuery(query);

        while (resultset.next()) {
            System.out.println(resultset.getString(1) + " " + resultset.getString(2));
        }
    }

    private static void executeFirst(Statement statement) throws SQLException {
        String query = "Select d.id, first gpa from student where d.id <> 1";
        ResultSet resultset = statement.executeQuery(query);

        while (resultset.next()) {
            System.out.println(resultset.getString(1) + " " + resultset.getString(2) + " " + resultset.getString(3));
        }
    }

    private static void executeTemporalize(Statement statement) throws SQLException {
        String query = "Temporalize student";
        statement.executeUpdate(query);
    }

    private static void executeTSelectOnDate(Statement statement) throws SQLException {
        String query = "tselect gpa from student where id = 1 and date = '2019-01-21' ";
        ResultSet resultset = statement.executeQuery(query);

        while (resultset.next()) {
            // can parse the results accordinly with user needs
            System.out.println("Current results are : " + resultset.getString(3) + " for student id " + resultset.getInt(7));
        }
    }

    private static void executePrevious(Statement statement) throws SQLException {
        String query = "Select previous veeru name from student where id=1;";
        ResultSet resultset = statement.executeQuery(query);

        while (resultset.next()) {
            System.out.println(resultset.getString(1) + " " + resultset.getString(2) + " " + resultset.getString(3) + " " + resultset.getString(4));
        }
    }

    private static void executeNext(Statement statement) throws SQLException {
        String query = "Select next veeru name from student where id=1;";
        ResultSet resultset = statement.executeQuery(query);

        while (resultset.next()) {
            System.out.println(resultset.getString(1) + " " + resultset.getString(2) + " " + resultset.getString(3) + " " + resultset.getString(4));
        }
    }

    private static void executeTjoin(Statement statement) throws SQLException {
        String query = "tselect employee e tjoin manager m on e.d_id = m.d_id ;";
        ResultSet resultset = statement.executeQuery(query);

        while (resultset.next()) {
            System.out.println(resultset.getString(1) + " " + resultset.getString(2) + " " + resultset.getString(3));
        }
    }
}
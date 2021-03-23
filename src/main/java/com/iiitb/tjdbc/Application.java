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
            executeTemporalize(statement);
            //executeFirst(statement);
            //exectuteLast(statement);
            connection.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static void exectuteLast(Statement statement) throws SQLException {
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
        String query = "Temporalize student";
        statement.executeUpdate(query);
    }
}

package com.iiitb.tjdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.iiitb.tjdbc.util.ConnectionDetails.*;

public class Application {

    public static void main(String[] args) {
        try {
            Class.forName(DRIVER);
            Connection connection = DriverManager.getConnection(getUrl(), USER, PASSWORD);
            Statement statement = connection.createStatement();
            ResultSet resultset = statement.executeQuery("select * from student");

            while (resultset.next()) {
                System.out.println(resultset.getString(1) + " " + resultset.getInt(2));
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static String getUrl() {
        return BASE_URL + DBNAME;
    }
}

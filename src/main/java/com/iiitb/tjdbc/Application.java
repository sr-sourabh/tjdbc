package com.iiitb.tjdbc;

import com.iiitb.tjdbc.core.TJdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.iiitb.tjdbc.util.CommonUtils.getUrl;
import static com.iiitb.tjdbc.util.ConnectionDetails.*;

public class Application {

    public static void main(String[] args) {
        try {
            Class.forName(DRIVER);
            Connection connection = DriverManager.getConnection(getUrl(), USER, PASSWORD);
            Statement statement = TJdbc.createStatement(connection);
            String query = "Select first salary from domain where name='ayush'";
            ResultSet resultset = statement.executeQuery(query);

            while (resultset.next()) {
                System.out.println(resultset.getString(1) + " " + resultset.getInt(2));
            }

            connection.close();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}

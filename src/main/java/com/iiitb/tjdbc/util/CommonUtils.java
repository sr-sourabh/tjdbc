package com.iiitb.tjdbc.util;

import static com.iiitb.tjdbc.util.ConnectionDetails.BASE_URL;
import static com.iiitb.tjdbc.util.ConnectionDetails.DBNAME;

public class CommonUtils {
    public static String getUrl() {
        return BASE_URL + DBNAME;
    }

    public static final String EXECUTE_QUERY = "executeQuery";
    public static final String EXECUTE_UPDATE = "executeUpdate";
}

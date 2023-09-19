package com.anven.clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class ClickHouseUtil {
    private static Connection connection;

    public static Connection getConn(String host, int port, String database) throws SQLException, ClassNotFoundException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String  address = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        connection = DriverManager.getConnection(address, "default", "1234qwer");
        return connection;
    }

    public void close() throws SQLException {
        connection.close();
    }
}

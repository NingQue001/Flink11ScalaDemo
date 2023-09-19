package com.anven.clickhouse;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

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

    /**
     * 负载均衡
     *
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static Connection getBalanceConn() throws SQLException, ClassNotFoundException {
        // 初始化驱动
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        // url
        String url = "jdbc:clickhouse://hadoop05:8123,hadoop04:8123/default";
        //设置JDBC参数
        ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        clickHouseProperties.setUser("default");
        clickHouseProperties.setPassword("1234qwer");
        //声明数据源
        BalancedClickhouseDataSource balanced = new BalancedClickhouseDataSource(url, clickHouseProperties);
        //对每个host进行ping操作, 排除不可用的dead连接
        balanced.actualize();
        //获得JDBC连接
        connection = balanced.getConnection();

        return connection;
    }

    public void close() throws SQLException {
        connection.close();
    }
}

package com.anven.clickhouse;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;


public class J_MyClickHouseUtil extends RichSinkFunction<J_User> {
    Connection connection = null;

    String sql;

    public J_MyClickHouseUtil(String sql) {
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = ClickHouseUtil.getConn("10.1.5.73", 8123, "tutorial");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(J_User user, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1, user.id);
        preparedStatement.setString(2, user.name);
        preparedStatement.setLong(3, user.age);
        preparedStatement.addBatch();

        long startTime = System.currentTimeMillis();
        int[] ints = preparedStatement.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
    }
}

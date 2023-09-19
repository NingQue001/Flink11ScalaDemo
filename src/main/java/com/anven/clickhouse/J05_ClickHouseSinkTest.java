package com.anven.clickhouse;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *      进入clickhouse-client
 *     use default;
 *     drop table if exists user_table;
 *     CREATE TABLE default.user_table(id UInt16, name String, age UInt16 ) ENGINE = TinyLog();
 *
 *     注意：运行时得先打开 nc -lk 7777 -- 只能打开一次，关闭后多次打开同一个sokcet，会导致多个进程，程序连接报错!!!
 *     1,zhangsan,10
 */
public class J05_ClickHouseSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        // source
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777, "\n");

        // Transform 操作
        SingleOutputStreamOperator<J_User> dataStream = inputStream.map(new MapFunction<String, J_User>() {
            @Override
            public J_User map(String data) throws Exception {
                String[] split = data.split(",");
                return J_User.of(Integer.parseInt(split[0]),
                        split[1],
                        Integer.parseInt(split[2]));
            }
        });

        // sink
        String sql = "INSERT INTO default.user_table (id, name, age) VALUES (?,?,?)";
        J_MyClickHouseUtil jdbcSink = new J_MyClickHouseUtil(sql);
        dataStream.addSink(jdbcSink);
        dataStream.print();

        env.execute("clickhouse sink test");
    }
}
package com.anven.jtableAndSql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

public class SqlTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,bsSettings);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        properties.setProperty("zookeeper.connect", "node01:2181,node02:2181,node03:2181");
        properties.setProperty("group.id", "test");

        // ingest a DataStream from an external source
        DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(new SourceFunction<Tuple3<Long, String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple3<Long, String, Integer>> out) throws Exception {
                while (true){
                    out.collect(new Tuple3<>(1L,"a",11));
                    Thread.sleep(1000L);
                }
            }
            @Override
            public void cancel() {

            }
        });
        Table table = tableEnv.fromDataStream(ds, $("user"), $("product"), $("amount"));
        DataStream<Tuple2<Boolean, Row>> dsRow = tableEnv.toRetractStream(table, Row.class);
        dsRow.print();
        env.execute();

    }
}

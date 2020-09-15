package com.anven.temporalTabJDemo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class TemporalTableFunDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple2.of("US Dolar", 68L));
        ratesHistoryData.add(Tuple2.of("Euro", 102L));
        ratesHistoryData.add(Tuple2.of("Yen", 1L));
        ratesHistoryData.add(Tuple2.of("Euro", 116L));
        ratesHistoryData.add(Tuple2.of("Euro", 119L));

        DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
        Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream
        , $("r_currency")
        , $("r_rate")
        , $("r_proctime").proctime());
        tEnv.createTemporaryView("RatesHistory", ratesHistory);

        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime"
        , "r_currency");
        tEnv.registerFunction("Rates", rates);

        // Flink SQL use Rates() Function
    }
}

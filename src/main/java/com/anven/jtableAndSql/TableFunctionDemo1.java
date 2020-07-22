package com.anven.jtableAndSql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class TableFunctionDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        tEnv.registerFunction("split", new Split(" ")); // register udf
        tEnv.registerFunction("duplicator1", new DuplicatorFunction1());
        tEnv.registerFunction("duplicator2", new DuplicatorFunction2());
        tEnv.registerFunction("flatten", new FlattenFunction());

        List<Tuple2<Long,String>> ordersData = new ArrayList<>();
        ordersData.add(Tuple2.of(2L, "Euro"));
        ordersData.add(Tuple2.of(1L, "US Dollar"));
        ordersData.add(Tuple2.of(50L, "Yen"));
        ordersData.add(Tuple2.of(3L, "Euro"));

        DataStream<Tuple2<Long,String>> ordersDataStream = env.fromCollection(ordersData);
        Table orders = tEnv.fromDataStream(ordersDataStream, "amount, currency, proctime.proctime");
        tEnv.registerTable("Orders", orders);


        String joinSql = "SELECT o.amount, o.currency, T.word, T.length FROM Orders as o ," +
                " LATERAL TABLE(split(currency)) as T(word, length)";
        String leftJoinSql = "SELECT o.amount, o.currency, T.word, T.length FROM Orders as o " +
                "LEFT JOIN LATERAL TABLE(split(currency)) as T(word, length) ON TRUE";
        String duplicateSql1 = "SELECT * FROM Orders as o , " +
                "LATERAL TABLE(duplicator1(amount))," +
                "LATERAL TABLE(duplicator1(currency))";
        String duplicateSql2 = "SELECT * FROM Orders as o , " +
                "LATERAL TABLE(duplicator2(o.amount, o.currency))";
        String sql3 = "SELECT * FROM Orders as o , " +
                "LATERAL TABLE(flatten(100,200,300))";
        //left join
        // LATERAL TABLE ???
        Table result = tEnv.sqlQuery(duplicateSql2);
        tEnv.toAppendStream(result, Row.class).print();

        env.execute("TableFunctionDemo1");
    }

    public static class Split extends TableFunction<Tuple2<String,Integer>> {
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str) {
            for (String s : str.split(separator)) {
                collect(new Tuple2<String,Integer>(s, s.length()));
            }
        }
    }

    /**
     * 接收不固定个数的int型参数,然后将所有数据依次返回
     */
    public static class FlattenFunction extends TableFunction<Integer>{
        public void eval(Integer... args){
            for (Integer i: args){
                collect(i);
            }
        }
    }

    /**
     * 注册多个eval方法，接收long或者string类型的参数，然后将他们转成string类型
     */
    public static class DuplicatorFunction1 extends TableFunction<String>{
        public void eval(Long i){
            eval(String.valueOf(i));
        }

        public void eval(String s){
            collect(s);
        }
    }

    /**
     * 通过注册指定返回值类型，flink 1.11 版本开始支持
     */
    @FunctionHint(output = @DataTypeHint("ROW< i LONG, s STRING >"))
    public static class DuplicatorFunction2 extends TableFunction<Row> {
        public void eval(Long i, String s) {
            collect(Row.of(i, s));
            collect(Row.of(i, s));
        }
    }
}











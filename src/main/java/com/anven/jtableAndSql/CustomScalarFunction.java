package com.anven.jtableAndSql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import sun.tools.jconsole.Tab;

import java.util.stream.Stream;

/**
 * 标量函数
 */
public class CustomScalarFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String className = StringFunction.class.getName();
        String sql = "create temporary function default_catalog.default_database.mystring"
                + " as '" + className + "'";
        tableEnv.sqlUpdate(sql);

        Table table = tableEnv.sqlQuery("select mystring(8888)");
        tableEnv.toAppendStream(table, Row.class).print();

        String[] functions = tableEnv.listFunctions();
        Stream.of(functions)
//                .filter(f -> f.startsWith("my"))
                .forEach(System.out::println);

        env.execute("CustomScalarFunction");
    }

    public static class StringFunction extends ScalarFunction {
        public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return o.toString();
        }
    }
}

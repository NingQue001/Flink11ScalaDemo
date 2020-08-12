package com.anven.jtableAndSql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class JavaBatchWordCount {
    public static void main(String[] args) throws Exception {
        // batch mode 使用的是旧版的planner
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        String path = JavaBatchWordCount.class
                .getClassLoader()
                .getResource("words.txt")
                .getPath();

        /**
         * How to get a table?
         */

        // 1. Table Descriptor
//        tEnv.connect(new FileSystem().path(path))
////                .withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
//                .withFormat(new Csv().fieldDelimiter('\n'))
//                .withSchema(new Schema().field("word", Types.STRING))
//                .createTemporaryTable("words");

        // 2. User defined table source
//        TableSource csvSource = new CsvTableSource(path, new String[]{"word"}, new TypeInformation[]{Types.STRING});
//        tEnv.registerTableSource("words", csvSource);

        // 3. Register a Dataset
        DataSource<Tuple1<String>> dataSet = env.readCsvFile(path)
//                .fieldDelimiter("\n")
//                .lineDelimiter(",")
                .includeFields(true)
                .ignoreInvalidLines()
                .types(String.class);
//        dataSet.print();
        tEnv.registerDataSet("words", dataSet, "word");

        Table table = tEnv.scan("words")
                .groupBy("word")
                .select("word, count(1) as count");

        tEnv.toDataSet(table, Row.class).print();

        
    }
}

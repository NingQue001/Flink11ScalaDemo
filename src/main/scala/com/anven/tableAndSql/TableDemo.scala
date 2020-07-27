//package com.anven.tableAndSql
//
//import java.util.Properties
//
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema
//import org.apache.flink.table.api.{EnvironmentSettings, Table}
//// implicit
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api._
//import org.apache.flink.api.scala._
//
//object TableDemo {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val bsSettings = EnvironmentSettings.newInstance()
//      .useBlinkPlanner()
//      .inStreamingMode()
//      .build()
////    val bsTableEnv = StreamTableEnvironment.create(env, bsSettings)
//    val bsTableEnv = TableEnvironment.create(bsSettings)
//
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
//    properties.setProperty("zookeeper.connect", "node01:2181,node02:2181,node03:2181")
//    properties.setProperty("group.id", "test")
//
//    val ds: DataStream[(String, Int)] = env.addSource(
//      new FlinkKafkaConsumer[String]("test",
//        new SimpleStringSchema(),
//        properties)
//    ).map(line => {
//      val strs = line.split(",")
//      val name = strs(0).toString
//      val age = Integer.valueOf(strs(1)).intValue()
//      (name,  age)
//    })
//
////    org.apache.flink.table.expressions.Expression
//
//    val table = bsTableEnv.createTemporaryView("pp", ds)
//
//    val sinkDDL =
//      """
//        CREATE TABLE people (
//        name VARCHAR,
//        age INT
//        ) WITH (
//                 'connector.type' = 'jdbc',
//                 'connector.url' = 'jdbc:mysql://localhost:3306/big',
//                 'connector.table' = 'people',
//                 'connector.username' = 'root',
//                 'connector.password' = '123',
//                 'connector.write.flush.max-rows' = '1'
//        )
//      """
//    bsTableEnv.sqlUpdate(sinkDDL);
//
//    bsTableEnv.sqlUpdate("insert into people " +
//      "select name,age from pp");
//
//
//    env.execute("TableDemo")
//  }
//}
//

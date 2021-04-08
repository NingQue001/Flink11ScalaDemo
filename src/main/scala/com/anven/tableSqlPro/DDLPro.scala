package com.anven.tableSqlPro

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

object DDLPro {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.executeSql("CREATE TABLE sensor " +
      "(id STRING, temperature DOUBLE, ts BIGINT) " +
      "WITH " +
      "('connector.type' = 'filesystem'," +
      "'connector.path' = 'file:///usr/local/data/flink/sensor.txt'," +
      "'format.type' = 'csv'" +
      ")")

    /*TODO 熟悉TableResult的使用*/
    val tableResult: TableResult = tableEnv.sqlQuery("SELECT * FROM sensor").execute()
//    tableResult.print()
    val it = tableResult.collect()
    while (it.hasNext) {
      val row1: Row = it.next()
      println(row1)
    }

    // env.execute("DDLPro")
  }
}

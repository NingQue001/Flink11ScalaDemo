package com.anven.tableAndSql

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

class TableDemo2 {
  def main(args: Array[String]): Unit = {
    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)

    val text: DataStream[String] = bsEnv
      .readTextFile("file:///usr/local/tmp/list.txt")
      .map(_.split(","))
  }
}

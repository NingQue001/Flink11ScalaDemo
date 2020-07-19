package com.anven.tableAndSql

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


object TableDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val text: DataStream[String] = env.readTextFile("file:///usr/local/tmp/list.txt")

    text.print()

    env.execute()
  }
}

package com.anven.tableSqlPro

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors._

/**
 *  1 Table & SQL
 *  2 回撤流：false 回撤 true insert
 */
object SqlPro {
  def main(args: Array[String]): Unit = {
    val filePath = "file:///usr/local/data/flink/sensor.txt"

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 1 连接外部数据源
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv) // 设置文件格式处理类
      .withSchema(new Schema() // 设置schema
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
        .field("ts", DataTypes.BIGINT())
      )
      .createTemporaryTable("inputTable") // 创建临时表

    // 2 表的查询转换
    val sensorTable: Table = tableEnv.from("inputTable")
    // 2.1 简单查询
    var resultTable = sensorTable
      .select('id, 'temperature, 'ts) // 这种写法需引入 import org.apache.flink.table.descriptors._
      .filter('id === "1") // 注意是三个等号
    // 2.2 聚合
    val aggTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count() as 'count) // 调用count()函数

    // 3 测试输出
    // Table隐式转化为Stream需引入 import org.apache.flink.table.api.bridge.scala._
    resultTable.toAppendStream[(String, Double, Long)].print("inputTable")
    // 3.1 回撤流(如果key存在，则产生一个delete和一个insert数据流）
    // false：retract message  true：add message
    aggTable.toRetractStream[(String, Long)].print("aggTable")

    env.execute("SqlPro")
  }
}

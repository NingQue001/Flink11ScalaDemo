package com.anven.statePro

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
 * Keyed State示例，State存储于Heap中
 */
object ExampleCountWindowAverage extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.fromCollection(List(
    (1L, 3L),
    (1L, 5L),
    (1L, 7L),
    (1L, 9L),
    (1L, 11L)
  )).keyBy(_._1)
    .flatMap(new CountWindowAverage)
    .print()

  env.execute("ExampleCountWindowAverage")
}

class CountWindowAverage extends RichFlatMapFunction[(Long, Long), (Long, Long)] {
  // 定义私有 ValueState 变量 数据类型是Tuple2
  // Java中变量可以不赋初始值，Scala中必须要显式指定，可以使用下划线让编译器帮你设置
  private var sum: ValueState[(Long, Long)] = _

  override def open(parameters: Configuration): Unit = {
    /*TODO 从RuntimeContext中获取ValueState*/
    sum = getRuntimeContext.getState(
      // 使用createTypeInformation方法，需要引入org.apache.flink.api.scala
      new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    )
  }

  override def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {
    // 得到当前ValueState的值
    val tmpCurrentSum = sum.value

    // 如果为空，则赋初始值
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0L)
    }

    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    sum.update(newSum)

    // 当元素的个数累计超过2，则计算平均值，并清空State
    if (newSum._1 >= 2) {
      out.collect((input._1, newSum._2 / newSum._1))
      sum.clear()
    }
  }
}

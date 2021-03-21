package com.anven.joiningPro;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkTumblingWindowsInnerJoinDemo {
    public static void main(String[] args) throws Exception {
        int windowSize = 10;
        long delay = 5002L;
        //wm = // 115000 - 5002 = 109998‬ <= 109999
        // 触发不了窗口，下面的数据能正常接收，不会产生迟到

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //为了测试方便，这里设置并行度为1 （实际生产中，只有当所有subTask中的窗口都触发，窗口才会触发）
        env.setParallelism(1);

        // 一、加载数据源
        SingleOutputStreamOperator<Tuple3<String, String, Long>> leftSource =
                env.addSource(new StreamDataSourceA()).name("SourceA");

        SingleOutputStreamOperator<Tuple3<String, String, Long>> rightSource =
                env.addSource(new StreamDataSourceB()).name("SourceB");

        // 二、设置WaterMark
        //（"a", "1", 1000）
        SingleOutputStreamOperator<Tuple3<String, String, Long>> leftStream =
                leftSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String,
                        String,
                        Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                });

        //（"a", "hangzhou", 6000）
        SingleOutputStreamOperator<Tuple3<String, String, Long>> rightStream =
                rightSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String,
                        String,
                        Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                });

        // 三、Inner Join之Tumbling Window Join
        // window join可以基于时间时间或者处理时间，interval join只能基于事件时间
        DataStream<Tuple5<String, String, String, Long, Long>> joined = leftStream.join(rightStream)
                //join条件相等的字段
//                .where(new LeftSelectKey())
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))  //划分窗口
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String,
                        String, String, Long, Long>>() {
                    @Override
                    public Tuple5<String, String, String, Long, Long> join(Tuple3<String, String, Long> first,
                                                                           Tuple3<String, String, Long> second) throws Exception {
                        // 两个流的key的值相等，并且在同一个窗口内
                        // (a, 1, "hangzhou", 1000001000, 1000006000)
                        return new Tuple5<>(first.f0, first.f1, second.f1, first.f2, second.f2);
                    }
                });

        joined.print();
        env.execute("FlinkTumblingWindowsInnerJoinDemo");
    }

    // leftStream获取join 的条件相等字段
    public static class LeftSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        @Override
        public String getKey(Tuple3<String, String, Long> value) throws Exception {
            return value.f0;
        }
    }

    public static class RightSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
        @Override
        public String getKey(Tuple3<String, String, Long> value) throws Exception {
            return value.f0;
        }
    }
}
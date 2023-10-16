package com.anven.cepDemo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class FlinkCEPTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<String> timeOutTag = new OutputTag<String>("timeOutTag") {};

        // 获取登录事件流，并提取时间戳、生成水位线
        KeyedStream<LoginEvent, String> stream = env
                .fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
                )
                .assignTimestampsAndWatermarks(
                        //处理乱序的情况 -- 水印策略：有界无序
                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<LoginEvent>() {
                                            @Override
                                            public long extractTimestamp(LoginEvent loginEvent, long l) {
                                                return loginEvent.timestamp;
                                            }
                                        }
                                )
                )
                .keyBy(r -> r.userId);

        // 1. 定义 Pattern（事件模式），连续的三个登录失败事件
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("first") // 以第一个登录失败事件开始
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("second") // 接着是第二个登录失败事件
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("third") // 接着是第三个登录失败事件
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                }).within(Time.seconds(5)); // 开窗：5s

        // 2. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);

        // 3. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
//        patternStream
//                .select(new PatternSelectFunction<LoginEvent, String>() {
//                        @Override
//                        public String select(Map<String, List<LoginEvent>> map) throws
//                                Exception {
//                            //根据上面定义的模式的名称依次得到和模式匹配得到的数据过滤出来进行处理
//                            LoginEvent first = map.get("first").get(0);
//                            LoginEvent second = map.get("second").get(0);
//                            LoginEvent third = map.get("third").get(0);
//                            return first.userId + " 连续三次登录失败！登录时间：" +
//                                    first.timestamp + ", " + second.timestamp + ", " + third.timestamp;
//                        }
//                    }
//                )
//                .print("连续登陆失败三次事件：");
        SingleOutputStreamOperator<Object> selectStream = patternStream.select(timeOutTag, new PatternTimeoutFunction<LoginEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<LoginEvent>> map, long timeoutTimestamp) throws Exception {
                        LoginEvent first = map.get("first").get(0);
                        return first.userId + " 超时处理：" +
                                first.timestamp + ", " + timeoutTimestamp;
                    }
                },
                new PatternSelectFunction<LoginEvent, Object>() {
                    @Override
                    public Object select(Map<String, List<LoginEvent>> map) throws Exception {
                        //根据上面定义的模式的名称依次得到和模式匹配得到的数据过滤出来进行处理
                        LoginEvent first = map.get("first").get(0);
                        LoginEvent second = map.get("second").get(0);
                        LoginEvent third = map.get("third").get(0);
                        return first.userId + " 连续三次登录失败！登录时间：" +
                                first.timestamp + ", " + second.timestamp + ", " + third.timestamp;
                    }
                });

        selectStream.print("连续登陆失败三次事件");

        selectStream.getSideOutput(timeOutTag).print("超时啦！！！");

        env.execute();
    }
}

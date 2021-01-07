package com.anven.cepDemo;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.apache.flink.api.java.tuple.Tuple3;
import java.util.List;
import java.util.Map;

/**
 * CEP
 * 核心：NFA(非确定性有限状态自动机，No-deterministic Finite Automation)
 *
 */
public class TestCEP {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 接收source并转换为DataStream
        DataStream<Tuple3<String, String, String>> myDataStream = env.addSource(new StaticSource()).map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                return new Tuple3<String, String, String>(jsonObject.getString("userid")
                        , jsonObject.getString("orderid")
                        , jsonObject.getString("behave"));
            }
        });

        /**
         * 定义一个规则
         * 接收到behave是order之后，下一个动作是pay才符合规则
         *
         * 该规则对超过规定时间仍然没有支付的数据无法匹配，下一步的目标是实现该功能！！！
         */
        Pattern<Tuple3<String, String, String>, Tuple3<String, String, String>> myPattern = Pattern.<Tuple3<String, String, String>>begin("start").where(
                new IterativeCondition<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> ctx) throws Exception {
                        return value.f2.equals("order");
                    }
                }
        ).next("next").where(
                new IterativeCondition<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> ctx) throws Exception {
                        return value.f2.equals("pay");
                    }
                }
        ).within(Time.seconds(5));

        PatternStream<Tuple3<String, String, String>> patternStream = CEP
                .pattern(myDataStream.keyBy(1), myPattern);

        // 定义超时的订单
        OutputTag<String> timeOutOrder = new OutputTag<String>("timeOutOrder") {};

        SingleOutputStreamOperator<String> select = patternStream.select(timeOutOrder, new PatternTimeoutFunction<Tuple3<String, String, String>, String>() {
                    @Override
                    public String timeout(Map<String, List<Tuple3<String, String, String>>> map, long l) throws Exception {
                        List<Tuple3<String, String, String>> start = map.get("start");
                        Tuple3<String, String, String> startTuple = start.get(0);
                        return "迟到的订单:[startTuple:" + startTuple.toString() + "]";
                    }
                }, new PatternSelectFunction<Tuple3<String, String, String>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple3<String, String, String>>> map) throws Exception {
                        // 匹配上第一个条件
                        List<Tuple3<String, String, String>> start = map.get("start");
                        // 匹配上第二个条件
                        List<Tuple3<String, String, String>> next = map.get("next");

                        Tuple3<String, String, String> nextTuple = next.get(0);
                        return "正常的订单：[nextTuple:" + nextTuple.toString() + "]";
                    }
                }
        );

        // 输出正常的订单
        select.print();

        // 输出超时数据的流
        DataStream<String> sideOutput = select.getSideOutput(timeOutOrder);
        sideOutput.print();

        env.execute("Test CEP");
    }
}

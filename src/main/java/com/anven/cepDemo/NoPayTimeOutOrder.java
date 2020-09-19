package com.anven.cepDemo;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class NoPayTimeOutOrder {
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
         * 定义超时未支付订单的pattern
         */
        Pattern<Tuple3<String, String, String>, Tuple3<String, String, String>> noPayTimeOutPattern = Pattern.<Tuple3<String, String, String>>begin("start").where(
                new IterativeCondition<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> ctx) throws Exception {
                        return "order".equals(value.f2);
                    }
                }
        ).notNext("end").where(
                new IterativeCondition<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> value, Context<Tuple3<String, String, String>> ctx) throws Exception {
                        return "pay".equals(value.f2);
                    }
                }
        ).within(Time.seconds(3));

        PatternStream<Tuple3<String, String, String>> patternStream = CEP.pattern(myDataStream.keyBy(0), noPayTimeOutPattern);

        SingleOutputStreamOperator<Object> noPayTimeOutStream = patternStream.select(
                new PatternSelectFunction<Tuple3<String, String, String>, Object>() {

                    @Override
                    public Object select(Map<String, List<Tuple3<String, String, String>>> map) throws Exception {
                        List<Tuple3<String, String, String>> start = map.get("start");
                        return "超时未支付订单： " + start.get(0).toString();
                    }
                }
        );

        noPayTimeOutStream.print();

        env.execute("NoPayTimeOutOrder");
    }
}

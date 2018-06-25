package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CustomerMsgFromKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        String topic = "test";
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties);

        DataStream<String> stream = see.addSource(flinkKafkaConsumer011);
        DataStream<Tuple2<String, Integer>> result = stream.flatMap(new EventMapper1()).keyBy(0).timeWindow(Time.seconds(5)).sum(1).forward();

        result.print();

        see.execute("test-kafka");
    }

    private static class EventMapper implements FlatMapFunction<String, Tuple2<String, String>> {
        @Override
        public void flatMap(String tweet, Collector<Tuple2<String, String>> out) throws Exception {
            Map<String, String> map = JacksonUtil.readValue(tweet, Map.class);
            out.collect(new Tuple2<>(map.get("username"), map.get("eventType")));
        }
    }

    private static class EventMapper1 implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String tweet, Collector<Tuple2<String, Integer>> out) throws Exception {
            Map<String, String> map = JacksonUtil.readValue(tweet, Map.class);
            out.collect(new Tuple2<>( map.get("eventType"), 1));
        }
    }

    private static class myWindowFunction implements WindowFunction<Tuple2<String, String>,Tuple2<String, Integer>,Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, String>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            Map<String,Tuple2<String, Integer>> map = new ConcurrentHashMap<>();


           /* list.stream()
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));*/
            StreamSupport.stream(input.spliterator(), true).forEach(t -> {
                Tuple2<String, Integer> tuple2 = map.get(t.f1);
                if (tuple2 != null) {
                    tuple2.f1 += 1;
                }else {
                    tuple2 = new Tuple2<>();
                    map.put(t.f1, tuple2);
                    tuple2.f0 = t.f1;
                    tuple2.f1 = 1;
                    out.collect(tuple2);
                }
            });

            /*input.forEach(t -> {

            });*/
        }
    }

}

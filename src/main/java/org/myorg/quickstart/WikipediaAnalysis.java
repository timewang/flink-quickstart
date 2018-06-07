package org.myorg.quickstart;


import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikipediaAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
                .keyBy((KeySelector<WikipediaEditEvent, String>) event -> event.getUser());

        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", 0L), (FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>) (acc, event) -> {
                    acc.f0 = event.getUser();
                    acc.f1 += event.getByteDiff();
                    return acc;
                });

        //result.print();

        result
                .map((MapFunction<Tuple2<String, Long>, String>) tuple -> tuple.toString())
                .addSink(new FlinkKafkaProducer011<String>("localhost:9092", "wiki-result", new SimpleStringSchema()));

        see.execute();

    }

}

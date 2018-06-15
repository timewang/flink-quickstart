package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class SafeDistrict {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> rawdata = env.readTextFile("E:\\work\\myproject\\github\\flink-quickstart\\dataset\\SacramentocrimeJanuary2006.csv");
        DataSet<Tuple2<String, Integer>> result = rawdata
                .flatMap(new Counter())//map the data and as district,1
                .groupBy(0) // group the data according to district
                .sum(1) // to count no. of crimes in a district
                .minBy(1); //to find out the minimum crime
        result.print();
    }

    public static class Counter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.split(",");
            if (tokens[2].contains("district")) {
                return;
            } else {
                out.collect(new Tuple2<>(tokens[2], 1));
            }
        }
    }
}

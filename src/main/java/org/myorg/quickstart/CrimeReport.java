package org.myorg.quickstart;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class CrimeReport {

    public static void main(String[] args) throws Exception {
        // obtain an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // include 方法，如：cdatetime	address	district	beat	grid	crimedescr	ucr_ncic_code	latitude	longitude
        // includeFields("0000011"),0 代表不取该字段，1 是取，这里就是取 crimedescr	ucr_ncic_code 两个字段
        DataSet<Tuple2<String, String>> rawdata =
                env.readCsvFile("E:\\work\\myproject\\github\\flink-quickstart\\dataset\\SacramentocrimeJanuary2006.csv").includeFields("0000011").ignoreFirstLine()
                        .types(String.class, String.class);
        // group by crimerecord and ucr_code  and count number of records per group
        rawdata.groupBy(0,1).reduceGroup(new CrimeCounter())
                // print the result
                .print();
    }
    public static class CrimeCounter implements GroupReduceFunction<Tuple2<String ,String>, Tuple3<String ,String, Integer>> {
        @Override
        public void reduce(Iterable<Tuple2<String, String>> records, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            String crimerecord = null;
            String ucr_code = null;
            int cnt = 0;
            // count number of tuples
            for(Tuple2<String, String> m : records) {
                crimerecord = m.f0;
                ucr_code = m.f1;
                // increase count
                cnt++;
            }
            // emit crimerecord, ucr_code, and count
            out.collect(new Tuple3<>(crimerecord, ucr_code, cnt));
        }
    }

}

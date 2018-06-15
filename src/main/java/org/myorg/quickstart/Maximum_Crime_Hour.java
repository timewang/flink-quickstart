package org.myorg.quickstart;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
public class Maximum_Crime_Hour {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet < Tuple1 < String >> rawdata = env.readCsvFile("E:\\work\\myproject\\github\\flink-quickstart\\dataset\\SacramentocrimeJanuary2006.csv")
                .includeFields("1000000").ignoreFirstLine()
                .types(String.class);
        rawdata.map(new TimeExtractor()) //map the data according to the MM/dd/yyyy HH
                .groupBy(0, 1) //group the data according to date & hour
                .sum(2) // sum on the field 2 to count
                .groupBy(0) //group the data according to field date
                .maxBy(0,2) //to find out the maximum on the basis of per day
                .print();  //print the result

    }

    public static class TimeExtractor implements MapFunction< Tuple1 < String >, Tuple3<String, String, Integer>> {

        @Override
        public Tuple3<String, String, Integer> map(Tuple1<String> time) throws Exception {
            SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH");
            SimpleDateFormat formatter2 = new SimpleDateFormat("MM/dd/yyyy HH:mm");

            String dateInString = time.f0;
            Date date = formatter2.parse(dateInString);

            String dateTokens[] = formatter.format(date).split(" ");

            return new Tuple3<>(dateTokens[0], dateTokens[1], 1);
        }
    }
}
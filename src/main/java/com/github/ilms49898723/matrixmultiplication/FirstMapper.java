package com.github.ilms49898723.matrixmultiplication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class FirstMapper extends MapReduceBase implements Mapper<Object, Text, IntWritable, MatrixValue> {
    private enum Counters { NUM_RECORDS }

    @Override
    public void map(Object o, Text text, OutputCollector<IntWritable, MatrixValue> outputCollector, Reporter reporter) throws IOException {
        String token = text.toString();
        String[] matrix = token.split(",");
        String m = matrix[0];
        int i = Integer.parseInt(matrix[1]);
        int j = Integer.parseInt(matrix[2]);
        int v = Integer.parseInt(matrix[3]);
        if (m.equalsIgnoreCase("m")) {
            IntWritable nextKey = new IntWritable(j);
            MatrixValue nextValue = new MatrixValue(0, i, v);
            outputCollector.collect(nextKey, nextValue);
        } else {
            IntWritable nextKey = new IntWritable(i);
            MatrixValue nextValue = new MatrixValue(1, j, v);
            outputCollector.collect(nextKey, nextValue);
        }
        reporter.incrCounter(Counters.NUM_RECORDS, 1);
    }
}

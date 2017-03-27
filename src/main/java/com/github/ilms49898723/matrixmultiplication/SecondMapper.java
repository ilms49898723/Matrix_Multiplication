package com.github.ilms49898723.matrixmultiplication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class SecondMapper extends MapReduceBase implements Mapper<Object, Text, MatrixIndex, IntWritable> {
    private enum Counters { NUM_RECORDS }

    @Override
    public void map(Object o, Text text, OutputCollector<MatrixIndex, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String token = text.toString();
        String[] matrix = token.split(",");
        int i = Integer.parseInt(matrix[0]);
        int j = Integer.parseInt(matrix[1]);
        int v = Integer.parseInt(matrix[2]);
        MatrixIndex nextKey = new MatrixIndex(i, j);
        IntWritable nextValue = new IntWritable(v);
        outputCollector.collect(nextKey, nextValue);
    }
}

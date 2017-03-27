package com.github.ilms49898723.matrixmultiplication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class SecondReducer extends MapReduceBase implements Reducer<MatrixIndex, IntWritable, MatrixIndex, IntWritable> {
    private enum Counter { NUM_RECORDS }

    @Override
    public void reduce(MatrixIndex matrixIndex, Iterator<IntWritable> iterator, OutputCollector<MatrixIndex, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int sum = 0;
        while (iterator.hasNext()) {
            IntWritable value = iterator.next();
            sum += value.get();
        }
        MatrixIndex nextKey = new MatrixIndex(matrixIndex.getI(), matrixIndex.getJ());
        IntWritable nextValue = new IntWritable(sum);
        outputCollector.collect(nextKey, nextValue);
    }
}

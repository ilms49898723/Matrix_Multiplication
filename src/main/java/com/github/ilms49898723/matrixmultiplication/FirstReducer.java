package com.github.ilms49898723.matrixmultiplication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FirstReducer extends MapReduceBase implements Reducer<IntWritable, MatrixValue, MatrixIndex, IntWritable> {
    private enum Counter { NUM_RECORDS }

    @Override
    public void reduce(IntWritable intWritable, Iterator<MatrixValue> iterator, OutputCollector<MatrixIndex, IntWritable> outputCollector, Reporter reporter) throws IOException {
        List<MatrixValue> mMatrix = new ArrayList<>();
        List<MatrixValue> nMatrix = new ArrayList<>();
        while (iterator.hasNext()) {
            MatrixValue next = iterator.next();
            MatrixValue matrixValue = new MatrixValue(next.getMatrix(), next.getIndex(), next.getValue());
            if (matrixValue.getMatrix() == 0) {
                mMatrix.add(matrixValue);
            } else {
                nMatrix.add(matrixValue);
            }
        }
        for (MatrixValue mv : mMatrix) {
            for (MatrixValue nv : nMatrix) {
                MatrixIndex nextKey = new MatrixIndex(mv.getIndex(), nv.getIndex());
                IntWritable nextValue = new IntWritable(mv.getValue() * nv.getValue());
                outputCollector.collect(nextKey, nextValue);
            }
        }
        reporter.incrCounter(Counter.NUM_RECORDS, 1);
    }
}

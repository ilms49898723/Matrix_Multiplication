package com.github.ilms49898723.matrixmultiplication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataOutputStream;
import java.io.IOException;

public class MatrixRecordWriter implements RecordWriter<MatrixIndex, IntWritable> {
    private DataOutputStream out;

    public MatrixRecordWriter(DataOutputStream stream) {
        out = stream;
    }

    @Override
    public void write(MatrixIndex matrixIndex, IntWritable intWritable) throws IOException {
        out.writeBytes(String.valueOf(matrixIndex.getI()));
        out.writeBytes(",");
        out.writeBytes(String.valueOf(matrixIndex.getJ()));
        out.writeBytes(",");
        out.writeBytes(String.valueOf(intWritable.get()));
        out.writeBytes("\r\n");
    }

    @Override
    public void close(Reporter reporter) throws IOException {

    }
}

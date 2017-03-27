package com.github.ilms49898723.matrixmultiplication;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class MatrixOutputFormat extends FileOutputFormat<MatrixIndex, IntWritable> {
    @Override
    public RecordWriter<MatrixIndex, IntWritable> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String name, Progressable progressable) throws IOException {
        Path path = FileOutputFormat.getOutputPath(jobConf);
        Path fullPath = new Path(path, name);
        FileSystem fs = path.getFileSystem(jobConf);
        FSDataOutputStream fileOut = fs.create(fullPath, true);
        return new MatrixRecordWriter(fileOut);
    }
}

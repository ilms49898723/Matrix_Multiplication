package com.github.ilms49898723.matrixmultiplication;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Hello world!
 *
 */
public class MatrixMultiplication {
    public static void main(String[] args) {
        try {
            String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: matrixmultiplication <in> <out>");
                System.exit(1);
            }
            JobConf preJobConf = new JobConf();
            preJobConf.setJobName("Pre Job");
            preJobConf.setJarByClass(MatrixMultiplication.class);
            preJobConf.setMapOutputKeyClass(IntWritable.class);
            preJobConf.setMapOutputValueClass(MatrixValue.class);
            preJobConf.setOutputKeyClass(MatrixIndex.class);
            preJobConf.setOutputValueClass(IntWritable.class);
            preJobConf.setMapperClass(FirstMapper.class);
            preJobConf.setReducerClass(FirstReducer.class);
            preJobConf.setInputFormat(TextInputFormat.class);
            preJobConf.setOutputFormat(MatrixOutputFormat.class);
            FileInputFormat.setInputPaths(preJobConf, new Path(args[0]));
            FileOutputFormat.setOutputPath(preJobConf, new Path("temptemptemp"));
            JobClient.runJob(preJobConf);
            JobConf jobConf = new JobConf();
            jobConf.setJobName("Sum Job");
            jobConf.setJarByClass(MatrixMultiplication.class);
            jobConf.setMapOutputKeyClass(MatrixIndex.class);
            jobConf.setMapOutputValueClass(IntWritable.class);
            jobConf.setOutputKeyClass(MatrixIndex.class);
            jobConf.setOutputValueClass(IntWritable.class);
            jobConf.setMapperClass(SecondMapper.class);
            jobConf.setReducerClass(SecondReducer.class);
            jobConf.setInputFormat(TextInputFormat.class);
            jobConf.setOutputFormat(MatrixOutputFormat.class);
            FileInputFormat.setInputPaths(jobConf, new Path("temptemptemp"));
            FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
            JobClient.runJob(jobConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

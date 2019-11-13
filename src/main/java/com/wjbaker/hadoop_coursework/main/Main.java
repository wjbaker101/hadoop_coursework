package com.wjbaker.hadoop_coursework.main;

import com.wjbaker.hadoop_coursework.mapper.TemperatureDifferenceMapper;
import com.wjbaker.hadoop_coursework.reducer.TemperatureDifferenceReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Main {

    public static void main(final String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        new Main(args);
    }

    private Main(final String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration config = new Configuration();

        config.set("mapred.textoutputformat.separator", ",");

        Job job = Job.getInstance(config, "hadoop_coursework");
        job.setJarByClass(Main.class);
        job.setMapperClass(TemperatureDifferenceMapper.class);
        job.setReducerClass(TemperatureDifferenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);

        MultipleOutputs.addNamedOutput(
                job, "oxford", TextOutputFormat.class, Text.class, DoubleWritable.class);

        MultipleOutputs.addNamedOutput(
                job, "waddington", TextOutputFormat.class, Text.class, DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

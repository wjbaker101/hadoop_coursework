package com.wjbaker.hadoop_coursework.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class WordCountMapperTest {

    private MapDriver<Object, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        WordCountMapper mapper = new WordCountMapper();
        this.mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperOutputsCorrectKeysAndValues() throws IOException {
        this.mapDriver.withInput(new Text("a"), new Text("US1ILMG0006,20180101,PRCP,0,,,N,"));
        this.mapDriver.withInput(new Text("b"), new Text("US1ILMG0006,20180101,SNOW,0,,,N,"));
        this.mapDriver.withInput(new Text("c"), new Text("US1ILKD0005,20180101,PRCP,0,,,N,"));
        this.mapDriver.withInput(new Text("c"), new Text("US1CTFR0025,20180101,SNWD,25,,,N,"));
        this.mapDriver.withInput(new Text("c"), new Text("US1CTFR0025,20180101,WESD,20,,,N,"));
        this.mapDriver.withInput(new Text("c"), new Text("US1CTFR0025,20180101,WESF,0,,,N,"));
        this.mapDriver.withInput(new Text("c"), new Text("UK000056225,20180101,TMAX,103,,,E,"));
        this.mapDriver.withInput(new Text("c"), new Text("UK000056225,20180101,TMIN,44,,,E,"));
        this.mapDriver.withInput(new Text("c"), new Text("UK000056225,20180101,PRCP,19,,,E,"));

        this.mapDriver.withOutput(new Text("UK000056225|20180101"), new IntWritable(103));
        this.mapDriver.withOutput(new Text("UK000056225|20180101"), new IntWritable(44));

        this.mapDriver.runTest();
    }
}

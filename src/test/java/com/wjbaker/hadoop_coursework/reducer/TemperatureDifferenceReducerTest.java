package com.wjbaker.hadoop_coursework.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TemperatureDifferenceReducerTest {

    private static final Text BLANK_STRING = new Text("");

    private ReduceDriver<Text, DoubleWritable, DoubleWritable, Text> reduceDriver;

    @Before
    public void setUp() {
        TemperatureDifferenceReducer temperatureDifferenceReducer = new TemperatureDifferenceReducer();
        this.reduceDriver = ReduceDriver.newReduceDriver(temperatureDifferenceReducer);
    }

    @Test
    public void testReducerProducesCorrectDifferences() throws IOException {
        List<DoubleWritable> values1 = IntStream.of(103, 44)
                .boxed()
                .map(DoubleWritable::new)
                .collect(Collectors.toList());

        List<DoubleWritable> values2 = IntStream.of(366, 78)
                .boxed()
                .map(DoubleWritable::new)
                .collect(Collectors.toList());

        Text key1 = new Text("UK000056225|20180101");
        Text key2 = new Text("UK000056225|20180102");

        this.reduceDriver.withInput(key1, values1);
        this.reduceDriver.withInput(key2, values2);

        this.reduceDriver.withOutput(new DoubleWritable(59), BLANK_STRING);
        this.reduceDriver.withOutput(new DoubleWritable(288), BLANK_STRING);

        this.reduceDriver.runTest();
    }

    @Test
    public void testReducerIgnoresKeysWithOnlyOneValue() throws IOException {
        List<DoubleWritable> values1 = IntStream.of(103)
                .boxed()
                .map(DoubleWritable::new)
                .collect(Collectors.toList());

        Text key1 = new Text("UK000056225|20180101");

        this.reduceDriver.withInput(key1, values1);

        // We expect no outputs here

        this.reduceDriver.runTest();
    }
}

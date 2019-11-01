package com.wjbaker.hadoop_coursework.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {

        // Create a variable to store the difference between the min and max temperatures
        // Default to null for when current key has not been encountered yet
        Integer difference = null;

        // Values will be either a min or max temperature, so subtract any values it encounters (there should only be 2
        // values anyway), this should end up being the difference
        for (final IntWritable value : values) {
            if (difference == null) {
                difference = value.get();
            }
            else {
                difference -= value.get();
            }
        }

        // Ignore the current iteration if the difference has not been calculated
        // Find the absolute value that was calculated, in case the min value was found before the max value
        if (difference != null) {
            int absoluteValue = Math.abs(difference);

            context.write(key, new IntWritable(absoluteValue));
        }
    }
}
package com.wjbaker.hadoop_coursework.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TemperatureDifferenceReducer extends Reducer<Text, IntWritable, IntWritable, Text> {

    private static final Text BLANK_STRING = new Text("");

    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {

        // Create a variable to store the difference between the min and max temperatures
        // Default to null for when current key has not been encountered yet
        Integer difference = null;

        // Create a counter to make sure there is at least 2 values for each key, 1 for TMIN and 1 for TMAX
        int count = 0;

        // Values will be either a min or max temperature, so subtract any values it encounters (there should only be 2
        // values anyway), this should end up being the difference
        for (final IntWritable value : values) {
            if (difference == null) {
                difference = value.get();
            }
            else {
                difference -= value.get();
            }

            count++;
        }

        // Ignore the current iteration if the difference has not been calculated
        // Find the absolute value that was calculated, in case the min value was found before the max value
        if (difference != null && count > 1) {
            int absoluteValue = Math.abs(difference);

            // Write the temperature difference as the key and a blank as the value so that one column will be produced
            context.write(new IntWritable(absoluteValue), BLANK_STRING);
        }
    }
}

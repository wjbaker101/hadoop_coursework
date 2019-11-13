package com.wjbaker.hadoop_coursework.reducer;

import com.wjbaker.hadoop_coursework.main.DataUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class TemperatureDifferenceReducer extends Reducer<Text, DoubleWritable, DoubleWritable, Text> {

    private static final Text BLANK_STRING = new Text("");

    private MultipleOutputs multipleOutputs;

    @Override
    public void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context)
            throws IOException, InterruptedException {

        // Create a variable to store the difference between the min and max temperatures
        // Default to null for when current key has not been encountered yet
        Double difference = null;

        // Create a counter to make sure there is at least 2 values for each key, 1 for TMIN and 1 for TMAX
        int count = 0;

        // Values will be either a min or max temperature, so subtract any values it encounters (there should only be 2
        // values anyway), this should end up being the difference
        for (final DoubleWritable value : values) {
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
            double absoluteValue = Math.abs(difference);

            // Write the temperature difference as the key and a blank as the value so that one column will be produced
            // Split the different stations into their own files when output
            if (key.toString().startsWith(DataUtils.STATION_ID_OXFORD)) {
                this.multipleOutputs.write("oxford", new DoubleWritable(absoluteValue), BLANK_STRING, "oxford");
            }

            if (key.toString().startsWith(DataUtils.STATION_ID_WADDINGTON)) {
                this.multipleOutputs.write("waddington", new DoubleWritable(absoluteValue), BLANK_STRING, "waddington");
            }
        }
    }

    @Override
    public void setup(final Context context) {
        this.multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    public void cleanup(final Context context) throws IOException, InterruptedException {
        this.multipleOutputs.close();
    }
}

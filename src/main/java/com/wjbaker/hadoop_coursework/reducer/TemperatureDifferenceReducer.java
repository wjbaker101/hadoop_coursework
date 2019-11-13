package com.wjbaker.hadoop_coursework.reducer;

import com.wjbaker.hadoop_coursework.main.DataUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class TemperatureDifferenceReducer extends Reducer<Text, DoubleWritable, DoubleWritable, NullWritable> {

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

        if (difference == null || count != 2) {
            this.writeToOutput(key.toString(), -1.0D);

            return;
        }

        this.writeToOutput(key.toString(), Math.abs(difference));
    }

    /**
     * Write the temperature difference as the key and a blank as the value so that one column will be produced.
     * Split the different stations into their own files when output.
     */
    private void writeToOutput(final String key, final double value) throws IOException, InterruptedException {
        if (key.startsWith(DataUtils.STATION_ID_OXFORD)) {
            this.multipleOutputs.write(
                    "oxford", new DoubleWritable(value), NullWritable.get(), "oxford");
        }

        if (key.startsWith(DataUtils.STATION_ID_WADDINGTON)) {
            this.multipleOutputs.write(
                    "waddington",
                    new DoubleWritable(value),
                    NullWritable.get(),
                    "waddington");
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

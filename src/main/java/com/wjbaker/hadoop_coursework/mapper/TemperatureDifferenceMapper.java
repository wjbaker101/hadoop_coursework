package com.wjbaker.hadoop_coursework.mapper;

import com.wjbaker.hadoop_coursework.main.DataUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TemperatureDifferenceMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private static final String OBSERVATION_TYPE_TEMPERATURE_MIN = "TMIN";
    private static final String OBSERVATION_TYPE_TEMPERATURE_MAX = "TMAX";

    @Override
    public void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {

        String[] split = value.toString().split(",");

        // Get the information we want from the line of data
        // [0] = Station ID
        // [1] = Date
        // [2] = Observation Type
        // [3] = Observation Value

        String stationID = split[0];
        String date = split[1];
        String observationType = split[2];
        int observationValue = Integer.parseInt(split[3]);

        double temperatureCelsius = observationValue / 10.0D;

        if (this.shouldAddToContext(stationID, observationType)) {
            context.write(this.getKey(stationID, date), new DoubleWritable(temperatureCelsius));
        }
    }

    /**
     * Only include the stations and observation types we want.
     */
    private boolean shouldAddToContext(final String stationID, final String observationType) {
        boolean isWantedStation = stationID.equals(DataUtils.STATION_ID_OXFORD)
                || stationID.equals(DataUtils.STATION_ID_WADDINGTON);

        boolean isWantedObservationType = observationType.equals(OBSERVATION_TYPE_TEMPERATURE_MIN)
                || observationType.equals(OBSERVATION_TYPE_TEMPERATURE_MAX);

        return isWantedStation && isWantedObservationType;
    }

    /**
     * Create a key for the current station and the date.
     */
    private Text getKey(final String stationID, final String date) {
        String data = String.format("%s|%s", stationID, date);

        return new Text(data);
    }
}

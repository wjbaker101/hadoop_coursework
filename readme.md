# Hadoop Coursework
**October - November 2019**

Final year university coursework for the Cloud Computing module, creating a Hadoop program for getting calculating the difference between the minimum and maximum temperatures of stations in the UK.

## Data Source

Data: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/

Documentation: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/readme.txt

## Building

```
mvn clean package
```

## Running

This Hadoop application is intended to be run in a Google Cloud Platform Dataproc cluster.

In order to do so run the following command in the Google Cloud terminal:

```
gcloud dataproc jobs submit hadoop --cluster hadoop-coursework --region=us-central1 --jar gs://hadoop_coursework-willbaker10198/input/hadoop_coursework.jar -- com.wjbaker.hadoop_coursework.main.Main gs://hadoop_coursework-willbaker10198/input/2018-data.csv gs://hadoop_coursework-willbaker10198/output
```

*Make sure the output directory does not exist at the root of the bucket.*

This makes an assumption that the the `2018-data.csv` and `hadoop_coursework.jar` files are present in a storage Bucket.

This will produce a CSV formatted file for Oxford and Waddington in the `output` directory.   

## Testing

Run tests under the `/src/test` directory:

```
mvn clean test
```

These will test the mapper and reducer functions.
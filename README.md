# Automated Traffic Counter

This repository contains the implementation of a tool that evaluates text files containg data about traffic measurements.

### Installing pre-requisites

`pip install pytest pyspark`

### Expected data file format

The expected format is a text file with two columns divided by a single space: 
- the first containing the timestamp of measurement;
- the second with the number of vehicles observed.

```
2021-12-01T05:00:00 5
2021-12-01T05:30:00 12
2021-12-01T06:00:00 14
2021-12-01T06:30:00 15
2021-12-01T07:00:00 25
```

**Assumption:** Every file processed will present the same format (machine-generated)

### Running

`python automated_traffic_counter.py <file_path>`

Where `<file_path>` is the path to the input data file.

The tool will process the data and return:

- Total number of cars seen throughout the whole dataset.
- Total number of cars seen per day.
- Top 3 intervals with the most cars seen, accounting for ties.
- The 1.5 hour period(s) with the least cars.

#### Example
##### Command
```python automated_traffic_counter.py input_data/input_data_1.txt```

##### Input data

```
2021-12-01T05:00:00 5
2021-12-01T05:30:00 12
2021-12-01T06:00:00 14
2021-12-01T06:30:00 15
2021-12-01T07:00:00 25
2021-12-01T07:30:00 46
2021-12-01T08:00:00 42
2021-12-01T15:00:00 9
2021-12-01T15:30:00 11
2021-12-01T23:30:00 0
2021-12-05T09:30:00 18
2021-12-05T10:30:00 15
2021-12-05T11:30:00 7
2021-12-05T12:30:00 6
2021-12-05T13:30:00 9
2021-12-05T14:30:00 11
2021-12-05T15:30:00 15
2021-12-08T18:00:00 33
2021-12-08T19:00:00 28
2021-12-08T20:00:00 25
2021-12-08T21:00:00 21
2021-12-08T22:00:00 16
2021-12-08T23:00:00 11
2021-12-09T00:00:00 4
```

##### Output
```
Total number of cars seen: 398                                                  

Total number of cars seen per day:
2021-12-01 179
2021-12-05 81
2021-12-08 134
2021-12-09 4

Top intervals with most cars seen:
2021-12-01 07:30:00 46
2021-12-01 08:00:00 42
2021-12-08 18:00:00 33
                                                                                
1.5 hour period(s) with least cars
2021-12-01 15:00:00 9
2021-12-01 15:30:00 11
2021-12-01 23:30:00 0
```

### Tests

`pytest test_automated_traffic_counter.py`
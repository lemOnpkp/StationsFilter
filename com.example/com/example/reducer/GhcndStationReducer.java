package com.example.reducer;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GhcndStationReducer extends Reducer<Text, Text, Text, Text> {
  protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    for (Text value : values) {
      String outputValue = key.toString() + "," + value.toString();
      context.write(null, new Text(outputValue));
    } 
  }
}

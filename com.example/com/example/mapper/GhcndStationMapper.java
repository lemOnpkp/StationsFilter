package com.example.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GhcndStationMapper extends Mapper<LongWritable, Text, Text, Text> {
  private final Map<String, String> stateCodeToStateNameMap = new HashMap<>();
  
  protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    String stateFilePath = conf.get("state.metadata.path");
    loadStateMetadata(conf, stateFilePath);
  }
  
  protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    String line = value.toString();
    if (line.startsWith("US") || line.startsWith("CA") || line.startsWith("CH")) {
      parseUSCAStation(line, context);
    } else if (line.startsWith("MX")) {
      parseMXStation(line, context);
    } 
  }
  
  private void parseUSCAStation(String line, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    String[] parts = line.split("\\s+");
    if (parts.length < 6)
      return; 
    String stationId = parts[0];
    String stateCode = parts[4];
    StringBuilder locationBuilder = new StringBuilder();
    for (int i = 5; i < parts.length && 
      !parts[i].matches("GSN|\\d{5}") && !parts[i].matches("HCN|\\d{5}") && !parts[i].matches("CRN|\\d{5}"); i++) {
      if (locationBuilder.length() > 0)
        locationBuilder.append(" "); 
      locationBuilder.append(parts[i]);
    } 
    String location = locationBuilder.toString();
    String country = "N/A";
    if (line.startsWith("US")) {
      country = "UNITED STATES";
    }
    else if (line.startsWith("CH")) {
        country = "CHINA";
      }
    else {
      country = "CANADA";
    } 
    String stateName = getStateNameBasedOnCode(stateCode);
    context.write(new Text(stationId), new Text(String.join(",", new CharSequence[] { stateName, location, country })));
  }
  
  private void parseMXStation(String line, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    String[] parts = line.split("\\s+");
    if (parts.length < 5)
      return; 
    String stationId = parts[0];
    StringBuilder locationBuilder = new StringBuilder(parts[4]);
    for (int i = 5; i < parts.length; i++) {
      if (parts[i].matches("GSN|\\d{5}") || parts[i].matches("HCN|\\d{5}") || parts[i].matches("CRN|\\d{5}"))
        break; 
      locationBuilder.append(" ").append(parts[i]);
    } 
    String location = locationBuilder.toString().trim();
    String country = "MEXICO";
    String state = "MX";
    context.write(new Text(stationId), new Text("MX," + location + "," + country));
  }
  
  private void loadStateMetadata(Configuration conf, String hdfsFilePath) throws IOException {
    Path path = new Path(hdfsFilePath);
    FileSystem fs = FileSystem.get(conf);
    BufferedReader reader = new BufferedReader(new InputStreamReader((InputStream)fs.open(path)));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split("\\s+", 2);
        if (parts.length == 2) {
          String stateCode = parts[0];
          String stateName = parts[1];
          this.stateCodeToStateNameMap.put(stateCode, stateName);
        } 
      } 
      reader.close();
    } catch (Throwable throwable) {
      try {
        reader.close();
      } catch (Throwable throwable1) {
        throwable.addSuppressed(throwable1);
      } 
      throw throwable;
    } 
  }
  
  private String getStateNameBasedOnCode(String stateCode) {
    return this.stateCodeToStateNameMap.getOrDefault(stateCode, stateCode);
  }
}

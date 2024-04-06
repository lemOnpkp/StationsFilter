package com.example;

import com.example.mapper.GhcndStationMapper;
import com.example.reducer.GhcndStationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GhcndStationDriver {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: GhcndStationDriver <input path> <output path>");
      System.exit(-1);
    } 
    Configuration conf = new Configuration();
    conf.set("state.metadata.path", args[2]);
    Job job = Job.getInstance(conf, "GHCN Data Processing");
    job.setJarByClass(GhcndStationDriver.class);
    job.setMapperClass(GhcndStationMapper.class);
    job.setReducerClass(GhcndStationReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

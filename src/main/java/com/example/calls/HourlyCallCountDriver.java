package com.example.calls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HourlyCallCountDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Expect exactly two arguments: input path and output path
        if (otherArgs.length != 2) {
            System.err.println("Usage: HourlyCallCount <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Hourly Call Reason Count");

        // Set the Jar By Class (tells Hadoop which JAR contains our job classes)
        job.setJarByClass(HourlyCallCountDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(HourlyCallCountMapper.class);
        job.setReducerClass(HourlyCallCountReducer.class);

        // Set output key and value types for the Mapper (intermediate output)
        // Mapper outputs a Text (composite key) and IntWritable (1)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set output key and value types for the Reducer (final output)
        // Reducer outputs a Text (composite key) and IntWritable (total count)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths from command line arguments
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // Set number of reducers (optional, 1 is generally fine for your dataset size)
        job.setNumReduceTasks(1);

        // Submit the job to YARN and wait for it to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

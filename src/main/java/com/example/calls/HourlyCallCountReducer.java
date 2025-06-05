package com.example.calls;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HourlyCallCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // Sum all the '1's for the current composite key (e.g., "17:EMS")
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        // Output: (e.g., "17:EMS", 500)
        context.write(key, result);
    }
}

package com.example.calls;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HourlyCallCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text compositeKey = new Text();

    // Define date formatters as class members for efficiency
    // Input format for "YYYY-MM-DD HH:MM:SS" from the CSV
    private SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // Output format to extract just the hour "HH" (24-hour format)
    private SimpleDateFormat outputHourFormat = new SimpleDateFormat("HH");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // --- Robustness Checks ---
        // 1. Skip empty lines
        if (line.trim().isEmpty()) {
            return;
        }

        // 2. Split the CSV line by comma. The 'title' is at index 4, 'timeStamp' at index 5.
        //    CSV Header: 'lat', 'lng', 'desc', 'zip', 'title', 'timeStamp', 'twp', 'addr', 'e'
        String[] columns = line.split(",");

        // 3. Check if the line has enough columns (at least up to 'timeStamp' at index 5)
        //    Also, skip the header row (assuming header's 'title' doesn't contain a colon)
        if (columns.length > 5 && !columns[4].equalsIgnoreCase("title")) {
            String title = columns[4].trim();       // Get the raw title column
            String timestampStr = columns[5].trim(); // Get the raw timestamp string

            // --- Extract Primary Reason from Title ---
            String reason = "OTHER"; // Default reason if no colon is found
            int colonIndex = title.indexOf(":");
            if (colonIndex != -1) {
                reason = title.substring(0, colonIndex).trim();
            } else {
                reason = title; // If no colon, use the whole title as reason
            }
            reason = reason.toUpperCase(); // Standardize reason (e.g., "ems" -> "EMS")

            // --- Extract Hour from Timestamp ---
            String hour = null;
            try {
                // Parse the timestamp string into a Java Date object
                Date date = inputDateFormat.parse(timestampStr);
                // Format the Date object to get just the hour as a string
                hour = outputHourFormat.format(date);
            } catch (ParseException e) {
                // If the timestamp can't be parsed, increment a counter for bad data
                // and skip this record to prevent job failure.
                context.getCounter("DataQuality", "InvalidTimestamps").increment(1);
                return;
            }

            // --- Create Composite Key ---
            // Combine hour and reason into a single key, e.g., "17:EMS"
            String finalKey = hour + ":" + reason;
            compositeKey.set(finalKey);

            // --- Emit (Key, Value) Pair ---
            // Output: (e.g., "17:EMS", 1)
            context.write(compositeKey, one);
        }
    }
}

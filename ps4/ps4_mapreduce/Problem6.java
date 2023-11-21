/*
 * Problem6.java
 * 
 * CS 460: Problem Set 4
 */


import java.io.IOException;
import java.time.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Problem6 {

    /*
     * This mapper maps each input line to a set of (word, 1) pairs,
     * with one pair for each word in the line.
     */
    public static class MyMapper extends
        Mapper<Object, Text, IntWritable, IntWritable>
    {
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            // Convert the Text object for the value to a String.
            String line = value.toString();

            int user_id = Integer.valueOf(line.split(",")[0]);

            // If there's a semicolon, then they have friend values
            if (line.contains(";")) {
                // split semicolon, take second half, split comma to an array
                String[] friend_list = line.split(";")[1].split(",");

                context.write(new IntWritable(user_id),
                    new IntWritable(friend_list.length));

            }

        }
    }


    public static class MyReducer extends
        Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
        public void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException
        {
            // Total the number of friends associated with the user.
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }

            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "problem 6");
        job1.setJarByClass(Problem6.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem 4.java for comments describing the calls

        job1.setMapperClass(MyMapper.class);
        job1.setReducerClass(MyReducer.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
    }
}
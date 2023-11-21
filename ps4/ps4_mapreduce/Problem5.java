/*
 * Problem5.java
 * 
 * CS 460: Problem Set 4
 */

import java.io.IOException;
import java.time.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/*
 * interfaces and classes for Hadoop data types that you may
 * need for some or all of the problems from PS 4
 */
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


public class Problem5 {

    /*
     * This mapper maps each input line to a set of (word, 1) pairs,
     * with one pair for each word in the line.
     */
    public static class MyMapper1 extends
        Mapper<Object, Text, Text, IntWritable>
    {
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            // Convert the Text object for the value to a String.
            String line = value.toString();

            // Split the line on commas
            String[] fields = line.split(",");

            // Email address is optional, but would be in fields[4] if present
            if (fields.length >= 5) {
                String addr = fields[4];
                // make sure addr is an address
                if (addr.matches(".+@.+\\..+")) { // regex to find x@x.x
                    String domain = addr.split("@|;")[1];// split on @ or ;, take the second piece
                    context.write(new Text(domain), new IntWritable(1));
                }
            }

        }
    }


    public static class MyReducer1 extends
        Reducer<Text, IntWritable, Text, LongWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException
        {
            // Total the list of values associated with the address.
            long count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }

            context.write(key, new LongWritable(count));
        }
    }

    /*
     * This mapper finds the domain with the most users
     */
    public static class MyMapper2 extends
        Mapper<Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            // Convert the Text object for the value to a string
            String line = value.toString();

            // write to context: constant, (domain, num users)
            context.write(new Text("domain sum"), new Text(line));
        }
    }


    public static class MyReducer2 extends
        Reducer<Text, Text, Text, LongWritable>
    {
        public void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException
        {
            String domain_max = "";
            long num_users_max = 0;

            for (Text val_text : values) {
                String val = val_text.toString();
                // Separate domain and num users by tab
                String[] val_split = val.split("\t");
                String domain = val_split[0];
                long num_users = Long.valueOf(val_split[1]);

                // if current domain has new max, overwrite previous domain/max
                if (num_users > num_users_max) {
                    num_users_max = num_users;
                    domain_max = domain;
                }
            }

            // write the winning domain and number of users
            context.write(new Text(domain_max),
                new LongWritable(num_users_max));
        }
    }

    public static void main(String[] args) throws Exception {
    /*
     * First job in a chain of two jobs
     */
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "problem 5");
        job1.setJarByClass(Problem5.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem 4.java for comments describing the calls

        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        //   job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);


        /*
         * Second job in a chain of two jobs
         */
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "problem 5");
        job2.setJarByClass(Problem5.class);

        /* CHANGE THE CLASS NAMES AS NEEDED IN THE METHOD CALLS BELOW */
        // See Problem 4.java for comments describing the calls

        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        //   job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);
    }
}
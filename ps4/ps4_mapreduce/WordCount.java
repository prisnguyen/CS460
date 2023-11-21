/*
 * WordCount.java
 *
 * a sample MapReduce program for counting how many times each word appears
 * in a text file
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WordCount {
    public static class MyMapper
      extends Mapper<Object, Text, Text, IntWritable> 
    {        
        /*
         * maps each input line to a set of (word, 1) pairs, 
         * with one pair for each word in the line.
         */
        public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException 
        {
            // Convert the Text object for the value to a String.
            String line = value.toString();

            // Split the line on the spaces to get an array containing
            // the individual words.
            String[] words = line.split(" ");

            // Process the words one at a time, writing a key-value pair 
            // for each of them.
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    public static class MyReducer 
      extends Reducer<Text, IntWritable, Text, LongWritable> 
    {
        /*
         * adds up the 1s for a given word and writes 
         * a (word, count) pair
         */
        public void reduce(Text key, Iterable<IntWritable> values, 
                           Context context) 
          throws IOException, InterruptedException 
        {
            // Total the list of values associated with the word.
            long count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }

            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        /* 
         * Configures and runs the MapReduce job for this problem,
         * using the Mapper and Reducer classes defined above.
         * 
         * IMPORTANT: This program is provided as a model. 
         * You should NOT actually try to run it!
         */

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        // Specifies the name of the outer class.
        job.setJarByClass(WordCount.class);

        // Specifies the names of the mapper and reducer classes.
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Sets the types for the keys and values output by the reducer.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Sets the types for the keys and values output by the mapper.
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Configure the type and location of the data being processed.
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Specify where the results should be stored.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

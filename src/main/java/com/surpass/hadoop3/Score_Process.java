package com.surpass.hadoop3;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by cwei@cz2r.com on 2016/12/8.
 */
public class Score_Process extends Configured implements Tool{
    /**
     * MAP
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString(); //输入的纯文本转换为String
            StringTokenizer stringTokenizer = new StringTokenizer(line, "\n");
            while (stringTokenizer.hasMoreTokens()) {
                StringTokenizer tokenizerLine = new StringTokenizer(stringTokenizer.nextToken());
                String strName = tokenizerLine.nextToken();
                String strScore = tokenizerLine.nextToken();
                Text name = new Text(strName);
                int scoreInt = Integer.parseInt(strScore);
                context.write(name,new IntWritable(scoreInt));
            }
        }
    }

    /**
     * Reduce
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            int count =0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                sum += iterator.next().get();
                count ++;
            }
            int avg = sum/count;
            context.write(key,new IntWritable(avg));
        }
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(Score_Process.class);
        job.setJobName("Score_Process");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean success = job.waitForCompletion(true);
        return success?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Score_Process(), args);
        System.exit(run);
    }
}

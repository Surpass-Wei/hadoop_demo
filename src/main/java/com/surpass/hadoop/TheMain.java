package com.surpass.hadoop;

import com.surpass.hadoop3.Score_Process;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Created by cwei@cz2r.com on 2016/12/7.
 */
public class TheMain extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, strings).getRemainingArgs();

        Job job = new Job(conf, "myTask");//任务名
        job.setJarByClass(TheMain.class);

        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));

        job.setMapperClass(Score_Process.Map.class);
        job.setReducerClass(Score_Process.Reduce.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new TheMain(), args);
        System.exit(result);
    }
}

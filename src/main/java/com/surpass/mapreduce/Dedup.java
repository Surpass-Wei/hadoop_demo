package com.surpass.mapreduce;

import com.surpass.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 去除重复行
 * Created by cwei@cz2r.com on 2016/12/12.
 */
public class Dedup {

    /**
     * Mapper类
     */
    public static class ClassifyMap extends Mapper<Object, Text, Text, Text> {
        private static Text line = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line = value;
            context.write(line,new Text(""));
        }
    }

    /**
     * Reduce类
     */
    public static class DedupReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\Apache\\hadoop-2.6.5");

        Configuration conf = new Configuration();
        //  获取输入输出路径
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //  如果输出目录已存在，则要事先删除，否则会报错
        FileUtil.delDir(otherArgs[1]);

        if(otherArgs.length!=2) {
            System.out.println("Usage: wordcount <input> <output>");
            System.exit(2);
        }

        Job job = new Job();
        job.setJarByClass(Dedup.class);
        job.setMapperClass(ClassifyMap.class);
        job.setCombinerClass(DedupReduce.class);
        job.setReducerClass(DedupReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

package com.surpass.mapreduce;

import com.surpass.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 计算出文件中每个单词的频数。要求按照单词字母顺序进行排序。每个单词和其频数占一行。单词和频数之间有间隔。
 * Created by cwei@cz2r.com on 2016/12/9.
 */
public class WordCount {

    /**
     * Mapper类
     * 设置输入类型为 <Object, Text>
     * 设置输出类型为 <Text, IntWritable>
     */
    private static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        // 用来记录当前的单词
        private Text word = new Text();

        // 恒定值：1，用来记录某单词出现过
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());    //分词
            while (stringTokenizer.hasMoreTokens()) {
                word.set(stringTokenizer.nextToken());  //将word设置为当前单词
                context.write(word, ONE);
            }
        }
    }

    /**
     * Reduce类
     * 设置输入类型为 <Text, IntWritable>
     * 设置输出类型为 <Text, IntWritable>
     */
    private static class CountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
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

        Job job = new Job(conf, "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(CountReduce.class);
        job.setReducerClass(CountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

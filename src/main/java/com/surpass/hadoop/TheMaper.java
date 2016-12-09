package com.surpass.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by cwei@cz2r.com on 2016/12/7.
 */
public class TheMaper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        try {
            String[] lineSplit = line.split("\t");
            String _id = lineSplit[0];
            String _price = lineSplit[2];
            String _date = lineSplit[4];

            context.write(new Text(_id), new Text(_date+"_"+_price));
        }catch (ArrayIndexOutOfBoundsException e) {
            System.out.println(e.getStackTrace());
        }

    }
}

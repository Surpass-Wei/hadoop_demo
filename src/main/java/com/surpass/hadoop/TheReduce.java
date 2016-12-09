package com.surpass.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cwei@cz2r.com on 2016/12/7.
 */
public class TheReduce extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) {
        List<String> list = new ArrayList<String>();
        for (Text value_1 : values) {
            list.add(value_1.toString());
        }
    }
}

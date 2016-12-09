package com.surpass.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by cwei@cz2r.com on 2016/12/8.
 */
public class UploadFile {
    public static void main(String[] args) {
        try {
            fileUp();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void fileUp() throws IOException {
        System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\Apache\\hadoop-common-2.2.0-bin-master");
        System.setProperty("HADOOP_USER_NAME", "root");
        String target = "hdfs://192.168.11.86:9000/file22.txt";
        String local = "D:\\Other_Projects\\hadoopobject\\input\\score.txt";
        FileInputStream in = new FileInputStream(new File(local));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(target), conf);
        OutputStream out = fs.create(new Path(target));
        IOUtils.copyBytes(in, out, 4096, true);
        System.out.println("上传完成");
    }
}

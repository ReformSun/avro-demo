package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestMain1 {
    public static void main(String[] args) {

    }

    public static void testMethod1() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "zbb");
        //创建输入流
        FileInputStream fis = new FileInputStream(new File("e:/hello.txt"));
        //创建输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/hello4.txt"), true);

        //拷贝
        IOUtils.copyBytes(fis, fos, conf);
        //关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
}

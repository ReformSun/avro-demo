package com.parqeuet.example;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;

public class TestReadHDFS {
    public static void readParquetFromHDFS(String ipAddr, String port, String filePath,String fileName){
        //1、声明readSupport
        GroupReadSupport groupReadSupport = new GroupReadSupport();
        Path path = new Path("hdfs://"+ipAddr+":"+port+"/"+filePath+"/"+fileName);

        //2、通过parquetReader读文件
        ParquetReader<Group> reader = null;
        try{
            reader = ParquetReader.builder(groupReadSupport,path).build();
            Group group = null;
            while((group = reader.read()) !=null){
                System.out.println(group);
            }
        }catch(IOException ioe){
            ioe.printStackTrace();
        }finally{
            try{
                reader.close();
            }catch(IOException ioe){
                ioe.printStackTrace();
            }
        }
    }

}

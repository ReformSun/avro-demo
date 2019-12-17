package com.parqeuet.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.IOException;

public class TestWriteHDFS {
    /**
     * 另一种方式定义格式
     */
    private static MessageType getMessageTypeFromCode (){
        MessageType messageType = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("age")
                .requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test1")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test2")
                .named("group1")
                .named("trigger");
        //System.out.println(messageType.toString());
        return messageType;
    }
    public static void writeParquetToHDFS(String ipAddr, String port, String filePath,String fileName){

        //1、声明parquet的messageType
        MessageType messageType = getMessageTypeFromCode();

        //2、声明parquetWriter
        Path path = new Path("hdfs://"+ipAddr+":"+port+"/"+filePath+"/"+fileName);
        System.out.println(path);
        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(messageType,configuration);
        GroupWriteSupport writeSupport = new GroupWriteSupport();

        //3、写数据
        ParquetWriter<Group> writer = null;
        try{
            writer = new ParquetWriter<Group>(path,
                    ParquetFileWriter.Mode.OVERWRITE,
                    writeSupport,
                    CompressionCodecName.UNCOMPRESSED,
                    128*1024*1024,
                    5*1024*1024,
                    5*1024*1024,
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetWriter.DEFAULT_WRITER_VERSION,
                    configuration);

            //4、构建parquet数据，封装成Group。
            for(int i = 0; i < 10; i ++){
                Group group = new SimpleGroupFactory(messageType).newGroup();
                group.append("name",i+"@ximalaya.com")
                        .append("id",i+"@id")
                        .append("age",i)
                        .addGroup("group1")
                        .append("test1","test1"+i)
                        .append("test2","test2"+i);
                writer.write(group);
            }
        }catch(IOException ioe){
            ioe.printStackTrace();
        }finally{
            if(writer != null) {
                try{
                    writer.close();
                }catch(IOException ioe){
                    ioe.printStackTrace();
                }
            }
        }
    }
}

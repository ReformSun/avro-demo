package com.parqeuet.example;

import com.avro.example.User;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class TestAvroMain {
    public static void main(String[] args) throws IOException {
//        testMethod1();
        testMethod2();
    }

    public static void testMethod1() throws IOException {
        final String schemaString = ReflectData.get().getSchema(User.class).toString();
        final Schema schema = new Schema.Parser().parse(schemaString);
        Path file = new Path(
                "file:///Users/apple/Desktop/server/data/parquet/user.parq");
        ParquetWriter<User> parquetWriter = AvroParquetWriter.<User>builder(file)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withSchema(schema)
                .withDataModel(ReflectData.get())
                .build();
        User user3 = User.newBuilder()
                .setName("Harry")
                .setFavoriteNumber(1)
                .setFavoriteColor("green")
                .build();
        User user2 = User.newBuilder()
                .setName("xiaoli")
                .setFavoriteNumber(1)
                .setFavoriteColor("green")
                .build();
        User user1 = User.newBuilder()
                .setName("xiaozhang")
                .setFavoriteNumber(1)
                .setFavoriteColor("green")
                .build();
        parquetWriter.write(user3);
        parquetWriter.write(user2);
        parquetWriter.write(user1);

        parquetWriter.close();

    }

    /**
     * 读文件
     */
    public static void testMethod2() throws IOException {
        Path file = new Path(
                "file:///Users/apple/Desktop/server/data/parquet/user.parq");
        ParquetReader<User> parquetReader = AvroParquetReader
                .<User>builder(file)
                .build();
        User user = null;
        while ((user = parquetReader.read()) != null){
            System.out.println(user.toString());
        }


    }
}

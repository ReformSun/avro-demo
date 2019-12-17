package com.parqeuet.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.example.Paper.schema;
import static org.apache.parquet.hadoop.example.ExampleParquetWriter.*;

/**
 *
 * https://www.jianshu.com/p/8b32d05cc80b
 *
 * 文件（File）一个 Parquet 文件，包括数据和元数据，如果在 HDFS 之上，数据就是分散存储在多个 HDFS Block 中
 *
 * 行组（row group）数据在水平方向上按行拆分为多个单元，每个单元就是所谓的 Row Group，即行组。这是一般列式存储都会
 *      有的结构设计。每一个行组包含一定的行数，Parquet 读写的时候会将整个行组缓存在内存中，因此更大尺寸的行组将会占用更
 *      多的缓存，并且记录占用空间比较小的 Schema 可以在每一个行组中存储更多的行。
 * 列块（Column Chunk）一个行组中的每一列对应的保存在一个列块中。行组中的所有列连续的存储在这个行组文件中，每一个列块中的
 *      值都是相同类型的，不同列块可能使用不同的算法进行压缩
 * 数据页（Data Page）每一个列块划分为多个数据页或者说页，一个页是最小的编码的单位，在同一个列块的不同页可能使用不同的编码方式
 *
 *
 * Parquet 文件有三种类型的元数据，分别是file metadata、column(chunk) metadata、page header metadata，每部分元数据包含的
 * 信息从上面图解中大概可以得知。
 */
public class TestMain1 {
    private static Logger logger = LoggerFactory
            .getLogger(TestMain1.class);
    private static String schemaStr = "message schema {" + "optional int64 log_id;"
            + "optional binary idc_id;" + "optional int64 house_id;"
            + "optional int64 src_ip_long;" + "optional int64 dest_ip_long;"
            + "optional int64 src_port;" + "optional int64 dest_port;"
            + "optional int32 protocol_type;" + "optional binary url64;"
            + "optional binary access_time;}";
    static MessageType schema = MessageTypeParser.parseMessageType(schemaStr);


    public static void main(String[] args) throws IOException {
        testSchema();
//        testParquetWriter();
//        testParquetReader();
    }

    public static void testSchema() throws IllegalArgumentException, IOException {
        Configuration configuration = new Configuration();
        // windows 下测试入库impala需要这个配置
        System.setProperty("hadoop.home.dir",
                "/Users/apple/Desktop/server/hadoop-2.7.7");
        ParquetMetadata readFooter = null;
        Path parquetFilePath = new Path("file:///Users/apple/Desktop/server/data/parquet/test.parq");
        readFooter = ParquetFileReader.readFooter(configuration,
                parquetFilePath, ParquetMetadataConverter.NO_FILTER);
        MessageType schema =readFooter.getFileMetaData().getSchema();
        System.out.println(schema.toString());
    }

    private static void testParquetWriter() throws IOException {
        Path file = new Path(
                "file:///Users/apple/Desktop/server/data/parquet/test.parq");
        ParquetWriter.Builder builder =
                builder(file).withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                //.withConf(configuration)
                .withType(schema);
		/*
		 * file, new GroupWriteSupport(), CompressionCodecName.SNAPPY, 256 *
		 * 1024 * 1024, 1 * 1024 * 1024, 512, true, false,
		 * ParquetProperties.WriterVersion.PARQUET_1_0, conf
		 */
        ParquetWriter<Group> writer = builder.build();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        String[] access_log = { "111111", "22222", "33333", "44444", "55555",
                "666666", "777777", "888888", "999999", "101010" };
        for(int i=0;i<1000;i++){
            writer.write(groupFactory.newGroup()
                    .append("log_id", Long.parseLong(access_log[0]))
                    .append("idc_id", access_log[1])
                    .append("house_id", Long.parseLong(access_log[2]))
                    .append("src_ip_long", Long.parseLong(access_log[3]))
                    .append("dest_ip_long", Long.parseLong(access_log[4]))
                    .append("src_port", Long.parseLong(access_log[5]))
                    .append("dest_port", Long.parseLong(access_log[6]))
                    .append("protocol_type", Integer.parseInt(access_log[7]))
                    .append("url64", access_log[8])
                    .append("access_time", access_log[9]));
        }
        writer.close();
    }

    private static void testParquetReader() throws IOException{
        Path file = new Path(
                "file:///Users/apple/Desktop/server/data/parquet/test.parq");
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), file);

        ParquetReader<Group> reader = builder.build();
        SimpleGroup group =(SimpleGroup) reader.read();
        System.out.println("schema:"+group.getType().toString());
        System.out.println("idc_id:"+group.getString(1, 0));
    }

    public static ParquetWriter getParquetWriter1(String path) throws IOException {
        Path file = new Path(path);
        ParquetWriter<Group> writer = null;
        Configuration configuration = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writer = new ParquetWriter<Group>(file,
                ParquetFileWriter.Mode.OVERWRITE, // 支持的文件写入方式 覆盖
                writeSupport,
                CompressionCodecName.UNCOMPRESSED,  // 支持的压缩方式
                128*1024*1024,      // 设置块大小
                5*1024*1024,        // 支持的页大小
                5*1024*1024,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                ParquetWriter.DEFAULT_WRITER_VERSION,
                configuration);
        return writer;
    }
    public static ParquetWriter getParquetWriter2(String path) throws IOException {
        Path file = new Path(path);
        ParquetWriter<Group> writer = null;
        ParquetWriter.Builder builder =
                builder(file).withWriteMode(ParquetFileWriter.Mode.CREATE)
                        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        //.withConf(configuration)
                        .withType(schema);
        writer = builder.build();
        return writer;
    }

}

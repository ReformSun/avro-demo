package com.parqeuet.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.Serializable;

public class TestMain2 {

    public static void main(String[] args) throws IOException {
        testMethod1();
//        testMethod2();
//        testMethod3();
    }

    /**
     * 写文件
     * @throws IOException
     */
    public static void testMethod1() throws IOException {
        final String schemaString = ReflectData.get().getSchema(Datum.class).toString();
        final Schema schema = new Schema.Parser().parse(schemaString);
        Path file = new Path(
                "file:///Users/apple/Desktop/server/data/parquet/datum.parq");
        ParquetWriter<Datum> parquetWriter = AvroParquetWriter.<Datum>builder(file)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withSchema(schema)
                .withDataModel(ReflectData.get())
                .build();
        parquetWriter.write(new Datum(1L,2L));
        parquetWriter.write(new Datum(1L,3L));
        parquetWriter.write(new Datum(1L,4L));
        parquetWriter.write(new Datum(1L,5L));
        parquetWriter.write(new Datum(3L,7L));

        parquetWriter.close();

    }

    /**
     * 读文件
     */
    public static void testMethod2() throws IOException {
        Path file = new Path(
                "file:///Users/apple/Desktop/server/data/parquet/datum.parq");
        ParquetReader<Datum> parquetReader = AvroParquetReader
                .<Datum>builder(file)
                .withDataModel(ReflectData.get())
                .build();
        Datum datum = null;
        while ((datum = parquetReader.read()) != null){
            System.out.println(datum.toString());
        }


    }

    public static void testMethod3()throws IOException{
        Path file = new Path(
                "file:///Users/apple/Desktop/server/data/parquet/datum.parq");
        ParquetReader<Group> parquetReader = ParquetReader.<Group>builder(new GroupReadSupport(),file).build();
        Group group = parquetReader.read();
        System.out.println(group.toString());
    }




    public static class Datum implements Serializable {

        public Long a;
        public Long b;

        public Datum() {}

        public Datum(Long a, Long b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public String toString() {
            return "a: " + a + " b: " + b;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Datum datum = (Datum) o;
            return b == datum.b && (a != null ? a.equals(datum.a) : datum.a == null);
        }

        @Override
        public int hashCode() {
            int result = a != null ? a.hashCode() : 0;
            result = 31 * result + b.hashCode();
            return result;
        }
    }
}

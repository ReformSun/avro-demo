package com.test;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroSerializerWithoutCodeGenerationTest {
    public static void main(String[] args) throws IOException {


    }

    public static void testMethod1() throws IOException {
        String avscFilePath = "./src/main/avro/user.avsc";
        Schema schema = new Schema.Parser().parse(new File(avscFilePath));

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Tony");
        user1.put("favorite_number", 18);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 3);
        user2.put("favorite_color", "red");

        File file = new File("./src/main/resources/user2.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
    }
}

package com.test;

import com.avro.example.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class AvroDeSerializerFlinkTest {
    /** The name of the key value pair generic record. */
    public static final String KEY_VALUE_PAIR_RECORD_NAME = "KeyValuePair";

    /** The namespace of the key value pair generic record. */
    public static final String KEY_VALUE_PAIR_RECORD_NAMESPACE = "org.apache.avro.mapreduce";
    /** The name of the generic record field containing the key. */
    public static final String KEY_FIELD = "key";

    /** The name of the generic record field containing the value. */
    public static final String VALUE_FIELD = "value";
    private static final String basePatn = "/Users/apple/Documents/GitHub/flink-1.8/LearnFlink/src/main/resources/avro/";
    public static void main(String[] args) throws IOException {
//        testMethod1();
        testMethod2();
    }
    public static void testMethod1() throws IOException{
        Schema longSchema = Schema.create(Schema.Type.LONG);
        Schema schema = Schema.createRecord(KEY_VALUE_PAIR_RECORD_NAME,
                "A key/value pair", KEY_VALUE_PAIR_RECORD_NAMESPACE, false);
        schema.setFields(Arrays.asList(new Schema.Field(KEY_FIELD,
                longSchema, "The key", null), new Schema.Field(VALUE_FIELD,
                longSchema, "The value", null)));
        System.out.println(schema.toString());
        File file = new File(basePatn + "2019-08-28/16/07/_part-2-0.avro");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord tuple2 = null;
        while (dataFileReader.hasNext()) {
            tuple2 = dataFileReader.next();
            System.out.println(tuple2);
        }
    }

    public static void testMethod2() throws IOException{
        String avscFilePath = "./src/main/avro/KeyValuePair.avsc";
        Schema schema = new Schema.Parser().parse(new File(avscFilePath));
        File file = new File(basePatn + "2019-08-28/16/39/part-2-0");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }
}

package com.test;

import com.avro.example.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroSerializerTest {
    public static void main(String[] args) throws IOException {



    }

    /**
     * 基本数据类型
     * null
     * boolean
     * int
     * long
     * float
     * double
     * bytes
     * string
     * @throws IOException
     */
    public static void testMethod1()throws IOException{
        User user1 = new User();
        user1.setName("Tom");
        user1.setFavoriteNumber(7);

        User user2 = new User("Jack", 15, "red");

        User user3 = User.newBuilder()
                .setName("Harry")
                .setFavoriteNumber(1)
                .setFavoriteColor("green")
                .build();

        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("./src/main/resources/users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
    }
}

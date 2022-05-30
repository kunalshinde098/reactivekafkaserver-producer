package com.flowdim.demo;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;

public class ProducerDemo {

    public static String getCurrentTimeFormat(){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));
        return dtf.format(now);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //create producer properties
        Properties properties = PropertiesLoaderUtils.loadAllProperties("application.properties");

        //properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create the producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        try {
            Integer i = 1;

            while (true) {
                Thread.sleep(1000);
                //create a producer record
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("reactive-test-topic", getCurrentTimeFormat());

                producer.send(record);
                producer.flush();
                i++;
            }
        }finally {
            producer.close();

        }

    }
}
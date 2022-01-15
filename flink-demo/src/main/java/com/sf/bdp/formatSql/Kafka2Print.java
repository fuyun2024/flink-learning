package com.sf.bdp.formatSql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Kafka2Print {

    public static void main(String[] args) {
        // main
        Configuration conf = new Configuration();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(streamEnv, blinkStreamSettings);


        streamEnv.setParallelism(2);
        streamEnv.enableCheckpointing(1000 * 10);


//        String kafkaSource = "CREATE TABLE kafkaSource (\n" +
//                "  the_kafka_key STRING,\n" +
//                "  id BIGINT,\n" +
//                "  id2 BIGINT,\n" +
//                "  name STRING, \n" +
//                "  price DOUBLE, \n" +
//                "  ts BIGINT, \n" +
//                "  dt TIMESTAMP(3)\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'avro_test2',\n" +
//                "  'properties.group.id' = 'testGroup',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'properties.bootstrap.servers' = '192.168.152.128:9092',\n" +
//                "  'key.format' = 'raw',\n" +
//                "  'key.fields' = 'the_kafka_key',\n" +
//                "  'value.format' = 'avro-confluent',\n" +
//                "  'value.avro-confluent.schema-registry.url' = 'http://192.168.152.128:8081'\n" +
//                ")";



        String kafkaSource = "CREATE TABLE kafkaSource (\n" +
                "  the_kafka_key STRING,\n" +
                "  id BIGINT,\n" +
                "  id2 BIGINT,\n" +
                "  name STRING, \n" +
                "  price DOUBLE, \n" +
                "  ts BIGINT, \n" +
                "  dt TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'avro_test3',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'properties.bootstrap.servers' = '192.168.152.128:9092',\n" +
                "  'key.format' = 'raw',\n" +
                "  'key.fields' = 'the_kafka_key',\n" +
                "  'value.format' = 'debezium-avro-confluent',\n" +
                "  'value.debezium-avro-confluent.schema-registry.url' = 'http://192.168.152.128:8081'\n" +
                ")";


        tEnv.executeSql(kafkaSource);
        TableResult tableResult = tEnv.executeSql("select * from kafkaSource");
        System.out.println(tableResult.getTableSchema());
        tableResult.print();
    }
}

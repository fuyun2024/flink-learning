package com.sf.bdp.formatSql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Kafka2Print2 {

    public static void main(String[] args) {
        // main
        Configuration conf = new Configuration();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(streamEnv, blinkStreamSettings);


        streamEnv.setParallelism(1);
        streamEnv.enableCheckpointing(1000 * 10);


//        String kafkaSource = "CREATE TABLE kafkaSource (\n" +
//                "  id BIGINT,\n" +
//                "  name STRING, \n" +
//                "  price BIGINT, \n" +
//                "  ts BIGINT, \n" +
//                "  dt STRING\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'qlh_test1_6',\n" +
//                "  'properties.group.id' = 'testGroup',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'properties.bootstrap.servers' = '192.168.152.128:9092',\n" +
//                "  'value.format' = 'avro-confluent',\n" +
//                "  'value.avro-confluent.schema-registry.url' = 'http://192.168.152.128:8081'\n" +
//                ")";



        String kafkaSource = "CREATE TABLE kafkaSource (\n" +
                "  id BIGINT,\n" +
                "  name STRING, \n" +
                "  price BIGINT, \n" +
                "  ts BIGINT, \n" +
                "  dt STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'qlh_test1_8',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'properties.bootstrap.servers' = '192.168.152.128:9092',\n" +
                "  'value.format' = 'debezium-avro-confluent',\n" +
                "  'value.debezium-avro-confluent.schema-registry.url' = 'http://192.168.152.128:8081'\n" +
                ")";


        tEnv.executeSql(kafkaSource);
        TableResult tableResult = tEnv.executeSql("select * from kafkaSource");
        System.out.println(tableResult.getTableSchema());
        tableResult.print();
    }
}

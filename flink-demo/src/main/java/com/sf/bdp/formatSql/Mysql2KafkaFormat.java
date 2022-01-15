package com.sf.bdp.formatSql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Mysql2KafkaFormat {

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


        String mysqlCdcTable = "CREATE TABLE mysql_cdc_source\n" +
                "(\n" +
                "    id          BIGINT,\n" +
                "    name        STRING,\n" +
                "    price       DOUBLE,\n" +
                "    ts          BIGINT,\n" +
                "    dt          TIMESTAMP(3),\n" +
                "    PRIMARY KEY(id) NOT ENFORCED\n\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://10.207.20.198:3306/qlh',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'sf123456',\n" +
                "    'table-name' = 'mysql_cdc_source'\n" +
                ")";


//        String mysqlSink = "CREATE TABLE avro_test1 (\n" +
//                "  the_kafka_key STRING,\n" +
//                "  id BIGINT,\n" +
//                "  name STRING, \n" +
//                "  price DOUBLE, \n" +
//                "  ts BIGINT, \n" +
//                "  dt TIMESTAMP(3)\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'avro_test1',\n" +
//                "  'properties.zookeeper.connect' = '192.168.152.128:2181',\n" +
//                "  'properties.bootstrap.servers' = '192.168.152.128:9092',\n" +
////                "  'format' = 'json'\n" +
////                "  'format' = 'avro'\n" +
////                "  'format' = 'csv'\n" +
////                "  'format' = 'debezium-json'\n" +
//                ")";



        String mysqlSink = "CREATE TABLE avro_test1 (\n" +
                "  the_kafka_key STRING,\n" +
                "  id BIGINT,\n" +
                "  id2 BIGINT,\n" +
                "  name STRING, \n" +
                "  price DOUBLE, \n" +
                "  ts BIGINT, \n" +
                "  dt TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'avro_test2',\n" +
                "  'properties.bootstrap.servers' = '192.168.152.128:9092',\n" +
                "  'key.format' = 'raw',\n" +
                "  'key.fields' = 'the_kafka_key',\n" +
                "  'value.format' = 'avro-confluent',\n" +
                "  'value.avro-confluent.schema-registry.url' = 'http://192.168.152.128:8081',\n" +
                "  'value.fields-include' = 'EXCEPT_KEY'\n" +
                ")";


//       String mysqlSink = "CREATE TABLE mysqlSink (\n" +
//                "    id        INT,\n" +
//                "    name        STRING,\n" +
//                "    price       DOUBLE,\n" +
//                "    ts          BIGINT,\n" +
//                "    dt          TIMESTAMP(3),\n" +
//                "    PRIMARY KEY(id) NOT ENFORCED\n\n" +
//                ") WITH (\n" +
//                "   'connector' = 'jdbc',\n" +
//                "   'url' = 'jdbc:mysql://10.207.20.198:3306/qlh',\n" +
//                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
//                "   'username' = 'root',\n" +
//                "   'password' = 'sf123456',\n" +
//                "   'table-name' = 'mysql_cdc_source2'\n" +
//                ")\n";


        tEnv.executeSql(mysqlCdcTable);
        tEnv.executeSql(mysqlSink);
        tEnv.executeSql("insert into avro_test1 " +
                " select cast(id as string) as the_kafka_key,id,id as id2,name,price,ts,dt from mysql_cdc_source");
    }
}

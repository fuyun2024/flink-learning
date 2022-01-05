package com.sf.bdp.formatSql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AvroConfluentTest2 {

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
        streamEnv.enableCheckpointing(1000 * 30);


        String mysqlCdcTable = "CREATE TABLE mysql_cdc_source\n" +
                "(\n" +
                "    id          INT,\n" +
                "    name        STRING,\n" +
                "    price       DOUBLE,\n" +
                "    ts          BIGINT,\n" +
                "    dt          TIMESTAMP(3),\n" +
                "    PRIMARY KEY(id) NOT ENFORCED\n\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = '10.207.20.198',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'sf123456',\n" +
                "    'database-name' = 'qlh',\n" +
                "    'table-name' = 'mysql_cdc_source',\n" +
                "    'scan.incremental.snapshot.chunk.size' = '4',\n" +
                "    'server-time-zone' = 'Asia/Shanghai'\n" +
                ")";


        String mysqlSink = "CREATE TABLE avro_test1 (\n" +
                "  the_kafka_key INT,\n" +
                "  id INT,\n" +
                "  name STRING, \n" +
                "  price DOUBLE, \n" +
                "  ts BIGINT, \n" +
                "  dt TIMESTAMP(3),\n" +
                "  PRIMARY KEY (the_kafka_key) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'avro_test1',\n" +
                "  'properties.bootstrap.servers' = '192.168.152.128:9092',\n" +
                "  'key.format' = 'raw',\n" +
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
        tEnv.executeSql("insert into avro_test1 select id as the_kafka_key,id,name,price,ts,dt from mysql_cdc_source");
    }
}

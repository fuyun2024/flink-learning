package com.sf.bdp.formatSql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlCdc2Kafka {

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


        String kafkaSink = "CREATE TABLE kafkaSink (\n" +
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
                "  'properties.bootstrap.servers' = '192.168.152.128:9092',\n" +
                "  'key.format' = 'raw',\n" +
                "  'key.fields' = 'the_kafka_key',\n" +
                "  'value.format' = 'debezium-avro-confluent',\n" +
                "  'value.debezium-avro-confluent.schema-registry.url' = 'http://192.168.152.128:8081'\n" +
                ")";


        tEnv.executeSql(mysqlCdcTable);
        tEnv.executeSql(kafkaSink);
        tEnv.executeSql("insert into kafkaSink " +
                " select cast(id as string) as the_kafka_key,id,id as id2,name,price,ts,dt from mysql_cdc_source");
    }
}

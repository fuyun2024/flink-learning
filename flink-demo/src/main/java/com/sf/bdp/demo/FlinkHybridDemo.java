package com.sf.bdp.demo;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkHybridDemo {


    public static void main(String[] args) {
        // main
        Configuration conf = new Configuration();
        conf.setString("state.checkpoints.num-retained", "3");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.207.20.198")
                .port(3306)
                .databaseList("qlh")
                .tableList("qlh.mysql_data")
//                .tableList(".*")
                .username("root")
                .password("sf123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .serverTimeZone("Asia/Shanghai")
                .build();


    }

}

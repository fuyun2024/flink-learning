package com.sf.bdp.flink;

import com.sf.bdp.flink.deserialization.DynamicRowRecordDeserializationSchema;
import com.sf.bdp.flink.entity.DynamicRowRecord;
import com.sf.bdp.flink.entity.DynamicSqlRecord;
import com.sf.bdp.flink.executor.JdbcBatchExecutor;
import com.sf.bdp.flink.out.JdbcBatchingOutputFormat;
import com.sf.bdp.flink.sink.GenericJdbcSinkFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class MySqlCdc2Mysql {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");
        conf.setString("state.checkpoints.num-retained", "3");
        conf.setString("state.checkpoints.dir", "file:\\" + System.getProperty("user.dir") + "\\checkpoint-dir");

        // 从某个 checkpoint 启动
//        conf.setString("execution.savepoint.path", "file:\\" + System.getProperty("user.dir")
//                + "\\checkpoint-dir\\59b16ff3a28c32c706c513cedc0199a5\\chk-5");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(1000 * 10);
        env.setParallelism(1);


        MySqlSource<Tuple2<String, DynamicRowRecord>> mySqlSource = MySqlSource.<Tuple2<String, DynamicRowRecord>>builder()
                .hostname("10.207.21.178")
                .port(3306)
                .databaseList("dbn2")
//                .tableList("dbn2.nim_account")
                .tableList(".*")
//                .tableList("dbn2.nim_pushcert", "dbn2.nim_pushkit_token", "dbn2.nim_queue_inst")
                .username("im_user")
                .password("Sf123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new DynamicRowRecordDeserializationSchema()) // converts SourceRecord to JSON Strinig
                .serverTimeZone("Asia/Shanghai")
//                .debeziumProperties(properties)
                .build();

        JdbcConnectionOptions options = new JdbcConnectionOptions(
                "jdbc:mysql://10.207.21.178:3306/",
                "com.mysql.cj.jdbc.Driver",
                "im_user",
                "Sf123456",
                30);

//        JdbcConnectionOptions options = new JdbcConnectionOptions(
//                "jdbc:mysql://10.207.20.198:3306/",
//                "com.mysql.cj.jdbc.Driver",
//                "root",
//                "sf123456",
//                30);


        JdbcBatchingOutputFormat<Tuple2<String, DynamicRowRecord>, DynamicSqlRecord, JdbcBatchExecutor<DynamicSqlRecord>>
                build = DynamicSqlOutputFormatBuilder.builder()
                .setOptions(options)
                .setMaxRetryTimes(3)
                .build();


        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .keyBy(t -> t.f0)
                .addSink(new GenericJdbcSinkFunction(build));

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
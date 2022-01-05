//package com.sf.bdp.cdc;
//
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//public class MySqlCdc2Mysql2 {
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        conf.setString("state.backend", "filesystem");
//        conf.setString("state.checkpoints.num-retained", "3");
//        conf.setString("state.checkpoints.dir", "file:\\" + System.getProperty("user.dir") + "\\checkpoint-dir");
//
//
//        // 从某个 checkpoint 启动
////        conf.setString("execution.savepoint.path", "file:\\" + System.getProperty("user.dir")
////                + "\\checkpoint-dir\\59b16ff3a28c32c706c513cedc0199a5\\chk-5");
//
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//        env.enableCheckpointing(1000 * 20);
//        env.setParallelism(10);
//
//
////        MySqlSource<Tuple2<String, DynamicRowRecord>> mySqlSource = MySqlSource.<Tuple2<String, DynamicRowRecord>>builder()
////                .hostname("10.207.20.198")
////                .port(3306)
////                .databaseList("qlh")
////                .tableList("qlh.mysql_data")
//////                .tableList(".*")
////                .username("root")
////                .password("sf123456")
////                .startupOptions(StartupOptions.specificOffset("mysql-bin.000001", 0))
////                .deserializer(new DynamicRowRecordDeserializationSchema())
////                .serverTimeZone("Asia/Shanghai")
////                .build();
//
//
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("10.207.20.198")
//                .port(3306)
//                .databaseList("qlh")
//                .tableList("qlh.mysql_data")
////                .tableList(".*")
//                .username("root")
//                .password("sf123456")
//                .startupOptions(StartupOptions.specificOffset("mysql-bin.000001", 0))
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .serverTimeZone("Asia/Shanghai")
//                .build();
//
//
////        JdbcConnectionOptions options = new JdbcConnectionOptions(
////                "jdbc:mysql://10.207.21.178:3306/?rewriteBatchedStatements=true",
////                "com.mysql.cj.jdbc.Driver",
////                "im_user",
////                "Sf123456",
////                30);
//
//        JdbcConnectionOptions options = new JdbcConnectionOptions(
//                "jdbc:mysql://10.207.20.198:3306/",
//                "com.mysql.cj.jdbc.Driver",
//                "root",
//                "sf123456",
//                30);
//
//        JdbcBatchingOutputFormat<Tuple2<String, DynamicRowRecord>, DynamicSqlRecord, JdbcBatchExecutor<DynamicSqlRecord>>
//                build = DynamicSqlOutputFormatBuilder.builder()
//                .setOptions(options)
//                .setFlushMaxSize(5000)
//                .setMaxRetryTimes(3)
//                .build();
//
//        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                // keyBy 保证表内有序，并且可以批量提交
////                .addSink(new GenericJdbcSinkFunction(build));
//                .print();
//
//
//        env.execute("Print MySQL Snapshot + Binlog");
//    }
//}
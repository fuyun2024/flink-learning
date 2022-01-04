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
        env.enableCheckpointing(1000 * 60);
        env.setParallelism(10);


        MySqlSource<Tuple2<String, DynamicRowRecord>> mySqlSource = MySqlSource.<Tuple2<String, DynamicRowRecord>>builder()
                .hostname("10.207.21.178")
                .port(3306)
                .databaseList("dbn1")
//                .tableList("dbn2.nim_account")
//                .tableList(".*")
                .tableList(
                        "dbn1.nim_account",
                        "dbn1.nim_account_dup",
                        "dbn1.nim_account_special",
                        "dbn1.nim_antispam_info",
                        "dbn1.nim_antispam_key",
                        "dbn1.nim_antispam_thesaurus",
                        "dbn1.nim_app_callback_config",
                        "dbn1.nim_av_signal_msg",
                        "dbn1.nim_av_signal_msg_copy",
                        "dbn1.nim_bima_appinfo",
                        "dbn1.nim_biz_tag",
                        "dbn1.nim_broadcast_msg",
                        "dbn1.nim_chatroom_info",
                        "dbn1.nim_cloud_account",
                        "dbn1.nim_config_history",
                        "dbn1.nim_crlist",
                        "dbn1.nim_crlist2",
                        "dbn1.nim_custom_kick_policy_config",
                        "dbn1.nim_datatunnel_config",
                        "dbn1.nim_ent_appinfo",
                        "dbn1.nim_ent_info",
                        "dbn1.nim_friend",
                        "dbn1.nim_friend_apply",
                        "dbn1.nim_history_session",
                        "dbn1.nim_job_info",
                        "dbn1.nim_kafka_topic_config",
                        "dbn1.nim_lastdevice",
                        "dbn1.nim_level_info",
                        "dbn1.nim_localhistory_url",
                        "dbn1.nim_mediaaudit_nosinfo",
                        "dbn1.nim_mqserver_inst",
                        "dbn1.nim_msg_biz_tag",
                        "dbn1.nim_msgroute_config",
                        "dbn1.nim_netcall_record",
                        "dbn1.nim_nos_del",
                        "dbn1.nim_nosinfo_ext",
                        "dbn1.nim_nrtc_license_account",
                        "dbn1.nim_nrtc_license_app_config",
                        "dbn1.nim_nrtc_license_package_info",
                        "dbn1.nim_nrtc_license_package_record",
                        "dbn1.nim_pushcert",
                        "dbn1.nim_pushkit_token",
                        "dbn1.nim_queue_inst",
                        "dbn1.nim_robot_info",
                        "dbn1.nim_routeconsumer_config",
                        "dbn1.nim_sdkgrant",
                        "dbn1.nim_special_relation",
                        "dbn1.nim_team_invite",
                        "dbn1.nim_team_msg_access",
                        "dbn1.nim_team_msg_ack",
                        "dbn1.nim_team_msg_ack_conf",
                        "dbn1.nim_team_msgid",
                        "dbn1.nim_tinfo",
                        "dbn1.nim_tlist",
                        "dbn1.nim_tlist2",
                        "dbn1.nim_tlist_20170701",
                        "dbn1.nim_tlist_20170706",
                        "dbn1.nim_tlist_msg",
                        "dbn1.nim_tokenmap",
                        "dbn1.nim_uinfo",
                        "dbn1.nim_uploadtask_record",
                        "dbn1.nim_yidun_config",
                        "dbn1.prd_uid")
                .username("im_user")
                .password("Sf123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new DynamicRowRecordDeserializationSchema()) // converts SourceRecord to JSON Strinig
                .serverTimeZone("Asia/Shanghai")
//                .debeziumProperties(properties)
                .build();


        JdbcConnectionOptions options = new JdbcConnectionOptions(
                "jdbc:mysql://10.207.21.178:3306/?rewriteBatchedStatements=true",
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
                .setFlushMaxSize(5000)
                .setMaxRetryTimes(3)
                .build();


        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // keyBy 保证表内有序，并且可以批量提交
                .keyBy(t -> t.f0)
                .addSink(new GenericJdbcSinkFunction(build));

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
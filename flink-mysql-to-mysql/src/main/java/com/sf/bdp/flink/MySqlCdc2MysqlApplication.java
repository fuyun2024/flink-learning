package com.sf.bdp.flink;

import com.sf.bdp.flink.deserialization.DynamicRowRecordDeserializationSchema;
import com.sf.bdp.flink.entity.DynamicRowRecord;
import com.sf.bdp.flink.entity.DynamicSqlRecord;
import com.sf.bdp.flink.executor.JdbcBatchExecutor;
import com.sf.bdp.flink.options.JdbcConnectionOptions;
import com.sf.bdp.flink.out.JdbcBatchingOutputFormat;
import com.sf.bdp.flink.sink.GenericJdbcSinkFunction;
import com.sf.bdp.flink.utils.PropertiesUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class MySqlCdc2MysqlApplication {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCdc2MysqlApplication.class);


    public static void main(String[] args) throws Exception {
        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));

        // 获取参数信息
        ApplicationParameter parameter = buildJobParameter(args);
        checkJobParameter(parameter);


        String[] databaseArray = Arrays.stream(parameter.getSourceDatabaseList().split(",")).toArray(String[]::new);
        String[] tableArray = Arrays.stream(parameter.getSourceTableList().split(",")).toArray(String[]::new);


        MySqlSource<Tuple2<String, DynamicRowRecord>> mySqlSource = MySqlSource.<Tuple2<String, DynamicRowRecord>>builder()
                .hostname(parameter.getSourceHostName())
                .port(Integer.valueOf(parameter.getSourcePort()))
                .databaseList(databaseArray)
                .tableList(tableArray)
                .username(parameter.getSinkUsername())
                .password(parameter.getSourcePassword())
                .startupOptions(StartupOptions.specificOffset(parameter.getSpecificOffsetFile(), Integer.valueOf(parameter.getSpecificOffsetPos())))
//                .startupOptions(StartupOptions.initial())
                .deserializer(new DynamicRowRecordDeserializationSchema())
                .serverTimeZone("Asia/Shanghai")
                .build();


        JdbcConnectionOptions options = new JdbcConnectionOptions(
                parameter.getSinkJdbcUrl(),
                parameter.getDriverName(),
                parameter.getSinkUsername(),
                parameter.getSinkPassword(),
                30);


        JdbcBatchingOutputFormat<Tuple2<String, DynamicRowRecord>, DynamicSqlRecord, JdbcBatchExecutor<DynamicSqlRecord>>
                build = DynamicSqlOutputFormatBuilder.builder()
                .setOptions(options)
                .setFlushMaxSize(Integer.valueOf(parameter.getSinkFlushMaxSize()))
                .setMaxRetryTimes(Integer.valueOf(parameter.getSinkMaxRetryTimes()))
                .setFlushIntervalMills(Integer.valueOf(parameter.getSinkFlushIntervalMills()) * 1000)
                .build();

        Configuration conf = new Configuration();
        conf.setString("state.checkpoints.num-retained", "3");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        setCheckPoint(env, parameter);
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // keyBy 保证表内有序，并且可以批量提交
                .keyBy(t -> t.f0)
                .addSink(new GenericJdbcSinkFunction(build));
        env.execute("Print MySQL Snapshot + Binlog");
    }


    private static void setCheckPoint(StreamExecutionEnvironment env, ApplicationParameter parameter) {
        // checkPoint 时间间隔
        env.enableCheckpointing(Long.valueOf(parameter.getCheckpointInterval()) * 1000);
        CheckpointConfig ckConfig = env.getCheckpointConfig();

        // checkpoint 模式
        if (StringUtils.isEmpty(parameter.getCheckpointingMode()) ||
                CheckpointingMode.EXACTLY_ONCE.name().equalsIgnoreCase(parameter.getCheckpointingMode())) {
            ckConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            LOG.info("本次CheckpointingMode模式 精确一次 即exactly-once");
        } else {
            ckConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            LOG.info("本次CheckpointingMode模式 至少一次 即AT_LEAST_ONCE");
        }

        // 默认超时10 minutes.
        ckConfig.setCheckpointTimeout(10 * 60 * 1000);

        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        ckConfig.setMinPauseBetweenCheckpoints(500);

        // 同一时间只允许进行一个检查点
        ckConfig.setMaxConcurrentCheckpoints(1);

        // 设置失败次数
        ckConfig.setTolerableCheckpointFailureNumber(5);

        // 保留 checkPoint
        ckConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置后端状态
        try {
            env.setStateBackend(new RocksDBStateBackend(parameter.getCheckpointDir(), true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void checkJobParameter(ApplicationParameter applicationParameter) {

    }


    private static ApplicationParameter buildJobParameter(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propertiesFile = parameterTool.get("propertiesFile");
        Preconditions.checkNotNull(propertiesFile, "-propertiesFile 参数不能为空");

//       return  PropertiesUtil.loadObjectByProperties( new JobParameter(), System.getProperty("user.dir") + "\\conf\\mysql2mysql.properties");
        return PropertiesUtil.loadObjectByProperties(new ApplicationParameter(), propertiesFile);
    }


}
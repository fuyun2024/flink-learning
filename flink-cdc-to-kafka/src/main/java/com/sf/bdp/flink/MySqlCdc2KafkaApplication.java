package com.sf.bdp.flink;

import com.alibaba.fastjson.JSON;
import com.sf.bdp.flink.deserialization.GenericRowRecordDeserialization;
import com.sf.bdp.flink.entity.GenericKafkaSerializationSchema;
import com.sf.bdp.flink.entity.GenericRowRecord;
import com.sf.bdp.flink.extractor.ProducerRecordExtractor;
import com.sf.bdp.flink.extractor.RecordExtractor;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class MySqlCdc2KafkaApplication {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCdc2KafkaApplication.class);


    public static void main(String[] args) throws Exception {
        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));

        // 获取参数信息
        ApplicationParameter parameter = buildJobParameter(args);
        validateJobParameter(parameter);


        // create mysqlCdcSource
        MySqlSource<Tuple2<String, GenericRowRecord>> mysqlCdcSource = createSource(parameter);


        // create kafkaSink
        FlinkKafkaProducer<Tuple2<String, GenericRowRecord>> kafkaSink = createSink(parameter);


        // main
        Configuration conf = new Configuration();
        conf.setString("state.checkpoints.num-retained", "3");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        setCheckPoint(env, parameter);
        env.fromSource(mysqlCdcSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // keyBy dbTable 保证表内有序
                .keyBy(t -> t.f0)
                // keyBy key 同一记录有序
//                .keyBy(t -> t.f1)
                .addSink(kafkaSink);

        env.execute("Print MySQL Snapshot + Binlog");
    }

    private static FlinkKafkaProducer<Tuple2<String, GenericRowRecord>> createSink(ApplicationParameter parameter) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", parameter.getSinkBootstrapServers());
        kafkaProperties.setProperty("transaction.max.timeout.ms", 15 * 60 * 1000 + "");
        kafkaProperties.setProperty("transaction.timeout.ms", Long.valueOf(parameter.getCheckpointInterval()) * 1000 + "");

        Map<String, String> tableTopicMap = JSON.parseObject(parameter.getDbTableTopicMap(), Map.class);
        RecordExtractor recordExtractor = new ProducerRecordExtractor(tableTopicMap);

        return new FlinkKafkaProducer<>("", new GenericKafkaSerializationSchema(recordExtractor),
                kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }


    private static MySqlSource<Tuple2<String, GenericRowRecord>> createSource(ApplicationParameter parameter) {
        String[] databaseArray = Arrays.stream(parameter.getSourceDatabaseList().split(",")).toArray(String[]::new);
        String[] tableArray = Arrays.stream(parameter.getSourceTableList().split(",")).toArray(String[]::new);

        return MySqlSource.<Tuple2<String, GenericRowRecord>>builder()
                .hostname(parameter.getSourceHostName())
                .port(Integer.valueOf(parameter.getSourcePort()))
                .databaseList(databaseArray)
                .tableList(tableArray)
                .username(parameter.getSourceUsername())
                .password(parameter.getSourcePassword())
//                .startupOptions(StartupOptions.specificOffset(parameter.getSpecificOffsetFile(), Integer.valueOf(parameter.getSpecificOffsetPos())))
                .startupOptions(StartupOptions.initial())
                .deserializer(new GenericRowRecordDeserialization())
                .serverTimeZone("Asia/Shanghai")
                .build();
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

    private static void validateJobParameter(ApplicationParameter applicationParameter) {
        // todo
    }


    private static ApplicationParameter buildJobParameter(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propertiesFile = parameterTool.get("propertiesFile");
        Preconditions.checkNotNull(propertiesFile, "-propertiesFile 参数不能为空");

//       return  PropertiesUtil.loadObjectByProperties( new JobParameter(), System.getProperty("user.dir") + "\\conf\\mysql2kafka.properties");
        return PropertiesUtil.loadObjectByProperties(new ApplicationParameter(), propertiesFile);
    }


}
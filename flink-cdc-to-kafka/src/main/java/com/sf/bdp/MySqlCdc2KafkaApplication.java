package com.sf.bdp;

import com.sf.bdp.entity.GenericCdcRecord;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.sf.bdp.utils.ApplicationHelper.*;

public class MySqlCdc2KafkaApplication {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCdc2KafkaApplication.class);


    public static void main(String[] args) throws Exception {
        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));

        // 获取参数信息
        ApplicationParameter parameter = buildJobParameter(args);
        validateJobParameter(parameter);


        // create mysqlCdcSource
        MySqlSource<Tuple2<String, GenericCdcRecord>> mysqlCdcSource = createSource(parameter);


        // create kafkaSink
        FlinkKafkaProducer<Tuple2<String, GenericCdcRecord>> kafkaSink = createSink(parameter);


        // main
        Configuration conf = new Configuration();
        conf.setString("state.checkpoints.num-retained", "3");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        setCheckPoint(env, parameter);
        env.setParallelism(4);
        env.fromSource(mysqlCdcSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // keyBy dbTable 保证表内有序
                .keyBy(t -> t.f0)
                // keyBy key 同一记录有序
                .keyBy(t -> t.f1.getKeyValueString())
                .addSink(kafkaSink);

        env.execute("Print MySQL Snapshot + Binlog");
    }


}
package com.sf.bdp;

import com.sf.bdp.record.GenericCdcRecord;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.sf.bdp.utils.ApplicationHelper.*;

public class ParallelMySqlCdc2KafkaApplication {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelMySqlCdc2KafkaApplication.class);


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

        String sourceName = "MysqlCdcSource";
        String sinkName = "KafkaSink";

        SingleOutputStreamOperator<Tuple2<String, GenericCdcRecord>> mySQLSource1 = env.fromSource(mysqlCdcSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .name(sourceName + "-1").uid(sourceName + "-1")
                .setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, GenericCdcRecord>> mySQLSource2 = env.fromSource(mysqlCdcSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .name(sourceName + "-2").uid(sourceName + "-2")
                .setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, GenericCdcRecord>> mySQLSource3 = env.fromSource(mysqlCdcSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .name(sourceName + "-3").uid(sourceName + "-3")
                .setParallelism(1);

        List<SingleOutputStreamOperator<Tuple2<String, GenericCdcRecord>>> sourceList = new ArrayList<>();
        sourceList.add(mySQLSource1);
        sourceList.add(mySQLSource2);
        sourceList.add(mySQLSource3);

        DataStream<Tuple2<String, GenericCdcRecord>> unionSource = sourceList.get(0);
        if (sourceList.size() > 1) {
            for (int i = 1; i < sourceList.size(); i++) {
                unionSource = unionSource.union(sourceList.get(i));
            }
        }

        unionSource
                // keyBy dbTable 保证表内有序
                .keyBy(t -> t.f0)
                // keyBy key 同一记录有序
                .keyBy(t -> t.f1.getKeyValueString())
//                .print().setParallelism(1);
                .addSink(kafkaSink).name(sinkName).uid(sinkName);

        env.execute("Print MySQL Snapshot + Binlog");
    }


}
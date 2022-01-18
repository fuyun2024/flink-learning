package com.sf.bdp.deserizlization.kafkaSource;

import com.sf.bdp.deserizlization.kafkaSource.provider.CachedSchemaCoderProvider;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Kafka2Print_DataStream {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka2Print_DataStream.class);


    public static void main(String[] args) throws Exception {
        String url = "http://192.168.152.128:8081";
        CachedSchemaCoderProvider schemaCoderProvider = new CachedSchemaCoderProvider(url, 1000);
        GenericAvroDeserializationSchema deserializationSchema = new GenericAvroDeserializationSchema(schemaCoderProvider);

        KafkaSource<GenericRecord> source = KafkaSource.<GenericRecord>builder()
                .setBootstrapServers("192.168.152.128:9092")
                .setTopics("qlh_test1_10")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(deserializationSchema)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 10);
        env.setParallelism(1);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .returns(TypeInformation.of(GenericRecord.class))
                .print();

        env.execute("Print MySQL Snapshot + Binlog");
    }


}
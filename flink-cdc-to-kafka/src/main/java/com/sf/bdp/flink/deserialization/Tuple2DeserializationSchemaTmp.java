package com.sf.bdp.flink.deserialization;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;

/**
 * description:
 * ------------------------------------
 * <p>
 * ------------------------------------
 * created by eHui on 2021/12/31
 */
public class Tuple2DeserializationSchemaTmp implements DebeziumDeserializationSchema<Tuple2<String, byte[]>> {

    private static final long serialVersionUID = -3168848963123670603L;

    private transient JsonConverter jsonConverter;

    public TypeInformation<Tuple2<String, byte[]>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, byte[]>>() {
        });
    }

    /**
     * flink cdc 输出的序列化器
     *
     * @param record 输入记录
     * @param out    数据记录， Tuple2<String, DynamicRowRecord> 类型
     * @throws Exception
     */
    public void deserialize(SourceRecord record, Collector<Tuple2<String, byte[]>> out) throws Exception {
        if (this.jsonConverter == null) {
            this.jsonConverter = new JsonConverter();
            HashMap<String, Object> configs = new HashMap(2);
            configs.put("converter.type", ConverterType.VALUE.getName());
//            configs.put("schemas.enable", this.includeSchema);
            this.jsonConverter.configure(configs);
        }

        byte[] bytes = this.jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());

        String dbTable = record.topic().substring(record.topic().indexOf(".") + 1);
        Tuple2<String, byte[]> tuple2 = new Tuple2<>(dbTable, bytes);
        out.collect(tuple2);
    }

}






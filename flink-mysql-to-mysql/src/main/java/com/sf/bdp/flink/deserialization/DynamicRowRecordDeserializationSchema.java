package com.sf.bdp.flink.deserialization;

import com.sf.bdp.flink.entity.DynamicRowRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.data.Schema.Type.BYTES;

/**
 * description:
 * ------------------------------------
 * <p>
 * ------------------------------------
 * created by eHui on 2021/12/31
 */
public class DynamicRowRecordDeserializationSchema implements DebeziumDeserializationSchema<Tuple2<String, DynamicRowRecord>> {

    private static final long serialVersionUID = -3168848963123670603L;


    public DynamicRowRecordDeserializationSchema() {

    }


    public TypeInformation<Tuple2<String, DynamicRowRecord>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, DynamicRowRecord>>() {
        });
    }

    /**
     * flink cdc 输出的序列化器
     *
     * @param sourceRecord 输入记录
     * @param out          数据记录， Tuple2<String, DynamicRowRecord> 类型
     * @throws Exception
     */
    public void deserialize(SourceRecord sourceRecord, Collector<Tuple2<String, DynamicRowRecord>> out) throws Exception {
        String dbTable = sourceRecord.topic().substring(sourceRecord.topic().indexOf(".") + 1);
        String[] split = dbTable.split("\\.");
        String dbName = split[0];
        String tableName = split[1];

        // todo
        // dbName = dbName + "_copy";


        Envelope.Operation op = Envelope.operationFor(sourceRecord);
        Struct value = (Struct) sourceRecord.value();
        Schema valueSchema = sourceRecord.valueSchema();

        DynamicRowRecord rowRecord;
        Record record;
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            record = extractAfterRow(value, valueSchema);
            rowRecord = DynamicRowRecord.buildUpsertRecord(dbName, tableName, RowKind.INSERT,
                    record.getFieldNames(), record.getFieldTypes(), record.getValues());
        } else if (op == Envelope.Operation.DELETE) {
            record = extractKeyRow((Struct) sourceRecord.key(), sourceRecord.keySchema());
            rowRecord = DynamicRowRecord.buildDeletedRecord(dbName, tableName, RowKind.DELETE,
                    record.getFieldNames(), record.getFieldTypes(), record.getValues());
        } else {
            record = extractAfterRow(value, valueSchema);
            rowRecord = DynamicRowRecord.buildUpsertRecord(dbName, tableName, RowKind.UPDATE_AFTER,
                    record.getFieldNames(), record.getFieldTypes(), record.getValues());
        }

        Tuple2<String, DynamicRowRecord> tuple2 = new Tuple2<>(dbTable, rowRecord);
        out.collect(tuple2);
    }


    private Record extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        return extractRow(after, afterSchema);
    }

    private Record extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        return extractRow(before, beforeSchema);
    }

    private Record extractKeyRow(Struct value, Schema valueSchema) throws Exception {
        return extractRow(value, valueSchema);
    }

    private Record extractRow(Struct after, Schema afterSchema) throws Exception {
        String[] fieldNames = afterSchema.fields().stream().map(Field::name)
                .collect(Collectors.toList()).toArray(new String[0]);

        Schema.Type[] fieldType = afterSchema.fields().stream().map(field -> field.schema().type())
                .collect(Collectors.toList()).toArray(new Schema.Type[0]);

        Object[] values = new Object[fieldType.length];

        for (int i = 0; i < fieldNames.length; i++) {
//            if (BYTES.equals(fieldType[i])) {
            if (BYTES.equals(fieldType[i]) && after.get(fieldNames[i]) instanceof ByteBuffer) {
                values[i] = convertToBinary.convert(after.get(fieldNames[i]), null);
            } else {
                values[i] = after.get(fieldNames[i]);
            }
        }

        return new Record(fieldNames, fieldType, values);
    }


    /**
     * byte 类型转换
     */
    static DeserializationRuntimeConverter convertToBinary = (dbzObj, schema) -> {
        if (dbzObj instanceof byte[]) {
            return dbzObj;
        } else if (dbzObj instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
        }
    };


    class Record {
        private final String[] fieldNames;
        private final Schema.Type[] fieldTypes;
        private final Object[] values;


        public Record(String[] fieldNames, Schema.Type[] fieldTypes, Object[] values) {
            this.fieldNames = fieldNames;
            this.fieldTypes = fieldTypes;
            this.values = values;
        }

        public String[] getFieldNames() {
            return fieldNames;
        }

        public Schema.Type[] getFieldTypes() {
            return fieldTypes;
        }

        public Object[] getValues() {
            return values;
        }
    }


}






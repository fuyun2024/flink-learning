package com.sf.bdp.deserialization;

import com.sf.bdp.entity.GenericCdcRecord;
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
public class GenericCdcRecordDeserializationSchema implements DebeziumDeserializationSchema<Tuple2<String, GenericCdcRecord>> {

    private static final long serialVersionUID = -3168848963123670603L;

    public TypeInformation<Tuple2<String, GenericCdcRecord>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, GenericCdcRecord>>() {
        });
    }

    /**
     * flink cdc 输出的序列化器
     *
     * @param sourceRecord 输入记录
     * @param out          数据记录， Tuple2<String, DynamicRowRecord> 类型
     * @throws Exception
     */
    public void deserialize(SourceRecord sourceRecord, Collector<Tuple2<String, GenericCdcRecord>> out) throws Exception {
        GenericCdcRecord genericCdcRecord = new GenericCdcRecord();

        setDbTable(genericCdcRecord, sourceRecord);

        //  todo 占时不处理
        setExtractKeyRow(genericCdcRecord, (Struct) sourceRecord.key(), sourceRecord.keySchema());

        Envelope.Operation op = Envelope.operationFor(sourceRecord);
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            setAfterRow(genericCdcRecord, (Struct) sourceRecord.value(), sourceRecord.valueSchema());
            genericCdcRecord.setKind(RowKind.INSERT);
        } else if (op == Envelope.Operation.DELETE) {
            setBeforeRow(genericCdcRecord, (Struct) sourceRecord.value(), sourceRecord.valueSchema());
            genericCdcRecord.setKind(RowKind.DELETE);
        } else {
            //  todo 占时不处理
            setBeforeRow(genericCdcRecord, (Struct) sourceRecord.value(), sourceRecord.valueSchema());
            genericCdcRecord.setKind(RowKind.UPDATE_BEFORE);
            emit(genericCdcRecord, out);

            setAfterRow(genericCdcRecord, (Struct) sourceRecord.value(), sourceRecord.valueSchema());
            genericCdcRecord.setKind(RowKind.UPDATE_AFTER);
        }

        emit(genericCdcRecord, out);
    }

    /**
     * 发送数据
     *
     * @param rowRecord
     * @param out
     */
    private void emit(GenericCdcRecord rowRecord, Collector<Tuple2<String, GenericCdcRecord>> out) {
        Tuple2<String, GenericCdcRecord> tuple2 = new Tuple2<>(rowRecord.getDbName() + "." + rowRecord.getTableName(),
                rowRecord);
        out.collect(tuple2);
    }


    private GenericCdcRecord setDbTable(GenericCdcRecord genericCdcRecord, SourceRecord sourceRecord) {
        String[] split = sourceRecord.topic().split("\\.");
        if (split.length != 3) {
            throw new IllegalArgumentException("topic name error : " + sourceRecord.topic());
        }

        genericCdcRecord.setDbName(split[1]);
        genericCdcRecord.setTableName(split[2]);
        return genericCdcRecord;
    }


    private GenericCdcRecord setBeforeRow(GenericCdcRecord genericCdcRecord, Struct value, Schema valueSchema) throws Exception {
        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);

        String[] fieldNames = beforeSchema.fields().stream().map(Field::name)
                .collect(Collectors.toList()).toArray(new String[0]);

        Schema.Type[] fieldType = beforeSchema.fields().stream().map(field -> field.schema().type())
                .collect(Collectors.toList()).toArray(new Schema.Type[0]);

        Object[] values = new Object[fieldType.length];
        for (int i = 0; i < fieldNames.length; i++) {
            if (BYTES.equals(fieldType[i]) && before.get(fieldNames[i]) instanceof ByteBuffer) {
                values[i] = convertToBinary.convert(before.get(fieldNames[i]), null);
            } else {
                values[i] = before.get(fieldNames[i]);
            }
        }

        genericCdcRecord.setFieldNames(fieldNames);
        genericCdcRecord.setFieldTypes(fieldType);
        genericCdcRecord.setValues(values);
        return genericCdcRecord;
    }


    private GenericCdcRecord setAfterRow(GenericCdcRecord genericCdcRecord, Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);

        String[] fieldNames = afterSchema.fields().stream().map(Field::name)
                .collect(Collectors.toList()).toArray(new String[0]);

        Schema.Type[] fieldType = afterSchema.fields().stream().map(field -> field.schema().type())
                .collect(Collectors.toList()).toArray(new Schema.Type[0]);

        Object[] values = new Object[fieldType.length];
        for (int i = 0; i < fieldNames.length; i++) {
            if (BYTES.equals(fieldType[i]) && after.get(fieldNames[i]) instanceof ByteBuffer) {
                values[i] = convertToBinary.convert(after.get(fieldNames[i]), null);
            } else {
                values[i] = after.get(fieldNames[i]);
            }
        }

        genericCdcRecord.setFieldNames(fieldNames);
        genericCdcRecord.setFieldTypes(fieldType);
        genericCdcRecord.setValues(values);
        return genericCdcRecord;
    }


    private GenericCdcRecord setExtractKeyRow(GenericCdcRecord genericCdcRecord, Struct value, Schema valueSchema) throws Exception {
        String[] fieldNames = valueSchema.fields().stream().map(Field::name)
                .collect(Collectors.toList()).toArray(new String[0]);

        Schema.Type[] fieldType = valueSchema.fields().stream().map(field -> field.schema().type())
                .collect(Collectors.toList()).toArray(new Schema.Type[0]);

        Object[] values = new Object[fieldType.length];
        for (int i = 0; i < fieldNames.length; i++) {
            if (BYTES.equals(fieldType[i]) && value.get(fieldNames[i]) instanceof ByteBuffer) {
                values[i] = convertToBinary.convert(value.get(fieldNames[i]), null);
            } else {
                values[i] = value.get(fieldNames[i]);
            }
        }

        genericCdcRecord.setKeyNames(fieldNames);
        genericCdcRecord.setKeyTypes(fieldType);
        genericCdcRecord.setKeyValues(values);
        return genericCdcRecord;
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


}






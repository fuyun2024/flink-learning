//package com.sf.bdp.extractor;
//
//import com.sf.bdp.entity.GenericCdcRecord;
//import com.sf.bdp.utils.RowTypeUtils;
//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericDatumWriter;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.io.BinaryEncoder;
//import org.apache.avro.io.EncoderFactory;
//import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.types.DataType;
//import org.apache.flink.table.types.logical.RowType;
//import org.apache.flink.util.WrappingRuntimeException;
//
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//public class DynamicRecordExtractor extends BaseRecordExtractor {
//
//    private WriteSchemaCoder.SchemaCoderProvider schemaCoderProvider;
//    protected WriteSchemaCoder schemaCoder;
//
//    private transient GenericDatumWriter datumWriter;
//    private transient ByteArrayOutputStream arrayOutputStream;
//    private transient BinaryEncoder encoder;
//
//
//    public DynamicRecordExtractor(Map<String, String> tableTopicMap, WriteSchemaCoder.SchemaCoderProvider schemaCoderProvider) {
//        super(tableTopicMap);
//        this.schemaCoderProvider = schemaCoderProvider;
//    }
//
//    @Override
//    protected byte[] extractorRecord(GenericCdcRecord record) {
//        checkAvroInitialized();
//
//        if (record == null) {
//            return null;
//        } else {
//            try {
//
//                arrayOutputStream.reset();
//
//                // 写入头文件
//                Schema schema = getSchema(record).getTypes().get(1);
//                schemaCoder.writeSchema(getTopicName(record.getDbTable()) + "-value", schema, arrayOutputStream);
//
//                // 写入数据文件
//                datumWriter.setSchema(schema);
//                datumWriter.write(buildGenericRecord(record, schema), encoder);
//
//                encoder.flush();
//                byte[] bytes = arrayOutputStream.toByteArray();
//                arrayOutputStream.reset();
//                return bytes;
//            } catch (IOException e) {
//                throw new WrappingRuntimeException("Failed to serialize schema registry.", e);
//            }
//        }
//    }
//
//
//    private GenericRecord buildGenericRecord(GenericCdcRecord record, Schema schema) {
//        GenericRecord genericRecord = new GenericData.Record(schema);
//        Object[] values = record.getValues();
//        String[] fieldNames = record.getFieldNames();
//
//        for (int i = 0; i < fieldNames.length; i++) {
//            genericRecord.put(fieldNames[i], values[i]);
//        }
//        return genericRecord;
//    }
//
//
//    private Schema getSchema(GenericCdcRecord record) {
//        String[] fieldNames = record.getFieldNames();
//        org.apache.kafka.connect.data.Schema.Type[] fieldTypes = record.getFieldTypes();
//
//        List<RowType.RowField> fields = new ArrayList<>();
//        for (int i = 0; i < fieldNames.length; i++) {
//            fields.add(RowTypeUtils.convertRowField(fieldNames[i], fieldTypes[i]));
//        }
//
//        RowType schema = new RowType(fields);
//
//        return AvroSchemaConverter.convertToSchema(schema);
//    }
//
//
//    public static RowType createDebeziumAvroRowType(DataType dataType) {
//        return (RowType)
//                DataTypes.ROW(
//                        DataTypes.FIELD("before", dataType.nullable()),
//                        DataTypes.FIELD("after", dataType.nullable()),
//                        DataTypes.FIELD("op", DataTypes.STRING()))
//                        .getLogicalType();
//    }
//
//
//    protected void checkAvroInitialized() {
//        if (datumWriter == null) {
//            ClassLoader cl = Thread.currentThread().getContextClassLoader();
//            GenericData genericData = new GenericData(cl);
//            datumWriter = new GenericDatumWriter(null, genericData);
//            arrayOutputStream = new ByteArrayOutputStream();
//            encoder = EncoderFactory.get().directBinaryEncoder(arrayOutputStream, (BinaryEncoder) null);
//
//            schemaCoder = schemaCoderProvider.get();
//        }
//    }
//
//}

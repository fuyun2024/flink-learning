package com.sf.bdp.extractor;

import com.sf.bdp.entity.GenericRowRecord;
import com.sf.bdp.extractor.coder.WriteSchemaCoder;
import com.sf.bdp.utils.RowTypeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.WrappingRuntimeException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

public class DynamicRecordExtractor2 extends BaseRecordExtractor {

    private WriteSchemaCoder.SchemaCoderProvider schemaCoderProvider;
    protected WriteSchemaCoder schemaCoder;

    private transient GenericDatumWriter datumWriter;
    private transient ByteArrayOutputStream arrayOutputStream;
    private transient BinaryEncoder encoder;

    private transient GenericRowData outputReuse;

    /** insert operation. */
    private static final StringData OP_INSERT = StringData.fromString("c");
    /** delete operation. */
    private static final StringData OP_DELETE = StringData.fromString("d");



    public DynamicRecordExtractor2(Map<String, String> tableTopicMap, WriteSchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        super(tableTopicMap);
        this.schemaCoderProvider = schemaCoderProvider;
    }

    @Override
    protected byte[] extractorRecord(GenericRowRecord record) {
        checkAvroInitialized();

        if (record == null) {
            return null;
        } else {
            try {

                Schema schema = getDebeziumSchema(record);
                GenericRecord genericRecord = buildGenericRecord(record, schema);

                arrayOutputStream.reset();

                // 写入头文件
                schemaCoder.writeSchema(getValueSubject(record.getDbTable()), schema, arrayOutputStream);

                // 写入数据文件
                datumWriter.setSchema(schema);
                datumWriter.write(genericRecord, encoder);

                encoder.flush();
                byte[] bytes = arrayOutputStream.toByteArray();
                arrayOutputStream.reset();
                return bytes;
            } catch (IOException e) {
                throw new WrappingRuntimeException("Failed to serialize schema registry.", e);
            }
        }
    }


    private GenericRecord buildGenericRecord(RowData rowData){
        try {
            switch (rowData.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    outputReuse.setField(0, null);
                    outputReuse.setField(1, rowData);
                    outputReuse.setField(2, OP_INSERT);
                case UPDATE_BEFORE:
                case DELETE:
                    outputReuse.setField(0, rowData);
                    outputReuse.setField(1, null);
                    outputReuse.setField(2, OP_DELETE);
                default:
                    throw new UnsupportedOperationException(
                            format(
                                    "Unsupported operation '%s' for row kind.",
                                    rowData.getRowKind()));
            }
        } catch (Throwable t) {
            throw new RuntimeException(format("Could not serialize row '%s'.", rowData), t);
        }
    }

    private GenericRecord buildGenericRecord(GenericRowRecord record, Schema schema) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        Object[] values = record.getValues();
        String[] fieldNames = record.getFieldNames();

        for (int i = 0; i < fieldNames.length; i++) {
            genericRecord.put(fieldNames[i], values[i]);
        }
        return genericRecord;
    }


    private Schema getDebeziumSchema(GenericRowRecord record) {
        RowType rowType = RowTypeUtils.getRowType(record);
        RowType debeziumAvroRowType = createDebeziumAvroRowType(fromLogicalToDataType(rowType));
        return AvroSchemaConverter.convertToSchema(debeziumAvroRowType);
    }


    public static RowType createDebeziumAvroRowType(DataType dataType) {
        return (RowType)
                DataTypes.ROW(
                        DataTypes.FIELD("before", dataType.nullable()),
                        DataTypes.FIELD("after", dataType.nullable()),
                        DataTypes.FIELD("op", DataTypes.STRING()))
                        .getLogicalType();
    }


    protected void checkAvroInitialized() {
        if (datumWriter == null) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            GenericData genericData = new GenericData(cl);
            datumWriter = new GenericDatumWriter(null, genericData);
            arrayOutputStream = new ByteArrayOutputStream();
            encoder = EncoderFactory.get().directBinaryEncoder(arrayOutputStream, (BinaryEncoder) null);

            schemaCoder = schemaCoderProvider.get();
        }
    }

}

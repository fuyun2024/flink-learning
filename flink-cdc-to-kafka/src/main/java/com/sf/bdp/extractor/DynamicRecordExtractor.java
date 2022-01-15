package com.sf.bdp.extractor;

import com.sf.bdp.entity.GenericRowRecord;
import com.sf.bdp.extractor.coder.WriteSchemaCoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.util.WrappingRuntimeException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class DynamicRecordExtractor extends BaseProducerRecordExtractor {

    private  WriteSchemaCoder.SchemaCoderProvider schemaCoderProvider;
    protected WriteSchemaCoder schemaCoder;

    private transient GenericDatumWriter datumWriter;
    private transient ByteArrayOutputStream arrayOutputStream;
    private transient BinaryEncoder encoder;


    public DynamicRecordExtractor(Map<String, String> tableTopicMap, WriteSchemaCoder.SchemaCoderProvider schemaCoderProvider) {
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
                arrayOutputStream.reset();

                // 写入头文件
                schemaCoder.writeSchema(getSubject(), getSchema(), arrayOutputStream);

                // 写入数据文件
                datumWriter.setSchema(getSchema());
                datumWriter.write(object, encoder);

                encoder.flush();
                byte[] bytes = this.arrayOutputStream.toByteArray();
                this.arrayOutputStream.reset();
                return bytes;
            } catch (IOException e) {
                throw new WrappingRuntimeException("Failed to serialize schema registry.", e);
            }
        }
    }


    private String getSubject() {
        return null;
    }


    private Schema getSchema() {
        return null;
    }


    protected void checkAvroInitialized() {
        if (this.datumWriter == null) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            GenericData genericData = new GenericData(cl);
            this.datumWriter = new GenericDatumWriter(null, genericData);
            this.arrayOutputStream = new ByteArrayOutputStream();
            this.encoder = EncoderFactory.get().directBinaryEncoder(this.arrayOutputStream, (BinaryEncoder)null);

            this.schemaCoder = this.schemaCoderProvider.get();
        }
    }

}

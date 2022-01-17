package com.sf.bdp.serialization;

import com.sf.bdp.entity.GenericAvroRecord;
import com.sf.bdp.serialization.provider.WriteSchemaCoder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.WrappingRuntimeException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GenericAvroSerializationSchema<In extends GenericAvroRecord> implements SerializationSchema<In> {

    protected WriteSchemaCoder.SchemaCoderProvider schemaCoderProvider;
    protected WriteSchemaCoder schemaCoder;

    private transient GenericDatumWriter datumWriter;
    private transient ByteArrayOutputStream arrayOutputStream;
    private transient BinaryEncoder encoder;


    public GenericAvroSerializationSchema(WriteSchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        this.schemaCoderProvider = schemaCoderProvider;
    }



    @Override
    public byte[] serialize(In record) {
        checkAndInitialized();

        if (record == null) {
            return null;
        } else {
            try {
                arrayOutputStream.reset();

                // 写入头文件
                schemaCoder.writeSchema(getValueSubject(record.getTopicName()), record.getSchema(), arrayOutputStream);

                // 写入数据文件
                datumWriter.setSchema(record.getSchema());
                datumWriter.write(record.getGenericRecord(), encoder);

                encoder.flush();
                byte[] bytes = arrayOutputStream.toByteArray();
                arrayOutputStream.reset();
                return bytes;
            } catch (IOException e) {
                throw new WrappingRuntimeException("Failed to serialize schema registry.", e);
            }
        }
    }

    /**
     * 初始化
     */
    private void checkAndInitialized() {
        if (datumWriter == null) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            GenericData genericData = new GenericData(cl);
            datumWriter = new GenericDatumWriter(null, genericData);
            arrayOutputStream = new ByteArrayOutputStream();
            encoder = EncoderFactory.get().directBinaryEncoder(arrayOutputStream, (BinaryEncoder) null);

            schemaCoder = schemaCoderProvider.get();
        }
    }


    public String getValueSubject(String topicName) {
        return topicName + "-value";
    }

}

package com.sf.bdp.deserizlization.kafkaSource;

import com.sf.bdp.deserizlization.kafkaSource.provider.MutableByteArrayInputStream;
import com.sf.bdp.deserizlization.kafkaSource.provider.ReadSchemaCoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;


public class GenericAvroDeserializationSchema implements DeserializationSchema<GenericRecord> {


    private final ReadSchemaCoder.SchemaCoderProvider schemaCoderProvider;
    private transient ReadSchemaCoder schemaCoder;
    private transient GenericDatumReader<GenericRecord> datumReader;
    private transient MutableByteArrayInputStream inputStream;
    private transient Decoder decoder;

    public GenericAvroDeserializationSchema(ReadSchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        this.schemaCoderProvider = schemaCoderProvider;
    }


    @Override
    public void open(InitializationContext context) throws Exception {

    }

    @Override
    public GenericRecord deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<GenericRecord> out) throws IOException {
        if (message == null) {
            return;
        }

        checkAvroInitialized();

        inputStream.reset();
        inputStream.setBuffer(message);

        Schema writerSchema = schemaCoder.readSchema(inputStream);

        datumReader.setSchema(writerSchema);
        datumReader.setExpected(writerSchema);

        GenericRecord genericRecord = datumReader.read(null, decoder);
        System.out.println("数据: " + genericRecord.toString());
//        out.collect(genericRecord);
    }

    @Override
    public boolean isEndOfStream(GenericRecord nextElement) {
        return false;
    }


    private void checkAvroInitialized() {
        if (datumReader != null) {
            return;
        }

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        GenericData genericData = new GenericData(cl);
        this.datumReader = new GenericDatumReader<>(null, null, genericData);
        this.inputStream = new MutableByteArrayInputStream();
        this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        if (schemaCoder == null) {
            this.schemaCoder = schemaCoderProvider.get();
        }
    }


    @Override
    public TypeInformation<GenericRecord> getProducedType() {
        return null;
    }
}

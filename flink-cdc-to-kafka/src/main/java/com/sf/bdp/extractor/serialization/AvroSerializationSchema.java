//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.sf.bdp.extractor.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingRuntimeException;

public class AvroSerializationSchema<T> implements SerializationSchema<T> {

    private static final long serialVersionUID = -8766681879020862312L;
    private Class<T> recordClazz;
    private String schemaString;
    private transient Schema schema;
    private transient GenericDatumWriter<T> datumWriter;
    private transient ByteArrayOutputStream arrayOutputStream;
    private transient BinaryEncoder encoder;

    public static <T extends SpecificRecord> AvroSerializationSchema<T> forSpecific(Class<T> tClass) {
        return new AvroSerializationSchema(tClass, (Schema)null);
    }

    public static AvroSerializationSchema<GenericRecord> forGeneric(Schema schema) {
        return new AvroSerializationSchema(GenericRecord.class, schema);
    }

    protected AvroSerializationSchema(Class<T> recordClazz, @Nullable Schema schema) {
        Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
        this.recordClazz = recordClazz;
        this.schema = schema;
        if (schema != null) {
            this.schemaString = schema.toString();
        } else {
            this.schemaString = null;
        }

    }

    public Schema getSchema() {
        return this.schema;
    }

    protected BinaryEncoder getEncoder() {
        return this.encoder;
    }

    protected GenericDatumWriter<T> getDatumWriter() {
        return this.datumWriter;
    }

    protected ByteArrayOutputStream getOutputStream() {
        return this.arrayOutputStream;
    }

    public byte[] serialize(T object) {
        this.checkAvroInitialized();
        if (object == null) {
            return null;
        } else {
            try {
                this.datumWriter.write(object, this.encoder);
                this.encoder.flush();
                byte[] bytes = this.arrayOutputStream.toByteArray();
                this.arrayOutputStream.reset();
                return bytes;
            } catch (IOException var3) {
                throw new WrappingRuntimeException("Failed to serialize schema registry.", var3);
            }
        }
    }

    protected void checkAvroInitialized() {
        if (this.datumWriter == null) {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            if (SpecificRecord.class.isAssignableFrom(this.recordClazz)) {
                Schema schema = SpecificData.get().getSchema(this.recordClazz);
                this.datumWriter = new SpecificDatumWriter(schema);
                this.schema = schema;
            } else {
                this.schema = (new Parser()).parse(this.schemaString);
                GenericData genericData = new GenericData(cl);
                this.datumWriter = new GenericDatumWriter(this.schema, genericData);
            }

            this.arrayOutputStream = new ByteArrayOutputStream();
            this.encoder = EncoderFactory.get().directBinaryEncoder(this.arrayOutputStream, (BinaryEncoder)null);
        }
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            AvroSerializationSchema<?> that = (AvroSerializationSchema)o;
            return this.recordClazz.equals(that.recordClazz) && Objects.equals(this.schema, that.schema);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.recordClazz, this.schema});
    }
}

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.sf.bdp.extractor.coder;

import com.sf.bdp.extractor.coder.WriteSchemaCoder;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class DynamicConfluentWriteSchemaRegistryCoder implements WriteSchemaCoder {

    private final SchemaRegistryClient schemaRegistryClient;

    public DynamicConfluentWriteSchemaRegistryCoder(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }


    public void writeSchema(String subject, Schema schema, OutputStream out) throws IOException {
        try {
            int registeredId = this.schemaRegistryClient.register(subject, schema);
            out.write(0);
            byte[] schemaIdBytes = ByteBuffer.allocate(4).putInt(registeredId).array();
            out.write(schemaIdBytes);
        } catch (RestClientException var5) {
            throw new IOException("Could not register schema in registry", var5);
        }
    }


}

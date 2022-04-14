/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sf.bdp.deserizlization.kafkaSource.provider;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.lang.String.format;

/**
 * Reads and Writes schema using Confluent Schema Registry protocol.
 */
public class DynamicConfluentReadSchemaRegistryCoder implements ReadSchemaCoder {

    private final SchemaRegistryClient schemaRegistryClient;
    private String subject;
    private static final int CONFLUENT_MAGIC_BYTE = 0;

    /**
     * Creates {@link ReadSchemaCoder} that uses provided {@link SchemaRegistryClient} to connect to
     * schema registry.
     *
     * @param schemaRegistryClient client to connect schema registry
     * @param subject              subject of schema registry to produce
     */
    public DynamicConfluentReadSchemaRegistryCoder(String subject, SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.subject = subject;
    }

    /**
     * Creates {@link ReadSchemaCoder} that uses provided {@link SchemaRegistryClient} to connect to
     * schema registry.
     *
     * @param schemaRegistryClient client to connect schema registry
     */
    public DynamicConfluentReadSchemaRegistryCoder(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    @Override
    public Schema readSchema(InputStream in) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(in);

        if (dataInputStream.readByte() != 0) {
            throw new IOException("Unknown data format. Magic number does not match");
        } else {
            int schemaId = dataInputStream.readInt();

            try {
                return schemaRegistryClient.getById(schemaId);
            } catch (RestClientException e) {
                throw new IOException(
                        format("Could not find schema with id %s in registry", schemaId), e);
            }
        }
    }

}

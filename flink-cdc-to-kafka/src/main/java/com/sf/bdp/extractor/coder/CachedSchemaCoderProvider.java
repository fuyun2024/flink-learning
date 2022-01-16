package com.sf.bdp.extractor.coder;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class CachedSchemaCoderProvider implements WriteSchemaCoder.SchemaCoderProvider {

    private static final long serialVersionUID = 8610401613495438381L;
    private final String url;
    private final int identityMapCapacity;
    @Nullable
    private final Map<String, ?> registryConfigs;

    public CachedSchemaCoderProvider(String url, int identityMapCapacity) {
        this(url, identityMapCapacity, (Map) null);
    }

    CachedSchemaCoderProvider(String url, int identityMapCapacity, @Nullable Map<String, ?> registryConfigs) {
        this.url = (String) Objects.requireNonNull(url);
        this.identityMapCapacity = identityMapCapacity;
        this.registryConfigs = registryConfigs;
    }

    public WriteSchemaCoder get() {
        return new DynamicConfluentWriteSchemaRegistryCoder(
                new CachedSchemaRegistryClient(this.url, this.identityMapCapacity, this.registryConfigs));
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            CachedSchemaCoderProvider that = (CachedSchemaCoderProvider) o;
            return this.identityMapCapacity == that.identityMapCapacity &&
                    this.url.equals(that.url) && Objects.equals(this.registryConfigs, that.registryConfigs);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.url, this.identityMapCapacity, this.registryConfigs});
    }
}
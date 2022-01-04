//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.ververica.cdc.connectors.mysql.source.config;

import com.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

@Internal
public class MySqlSourceConfigFactory implements Serializable {
    private static final long serialVersionUID = 1L;
    private int port = 3306;
    private String hostname;
    private String username;
    private String password;
    private ServerIdRange serverIdRange;
    private List<String> databaseList;
    private List<String> tableList;
    private String serverTimeZone;
    private StartupOptions startupOptions;
    private int splitSize;
    private int splitMetaGroupSize;
    private int fetchSize;
    private Duration connectTimeout;
    private int connectMaxRetries;
    private int connectionPoolSize;
    private double distributionFactorUpper;
    private double distributionFactorLower;
    private boolean includeSchemaChanges;
    private Properties dbzProperties;

    public MySqlSourceConfigFactory() {
        this.serverTimeZone = (String) MySqlSourceOptions.SERVER_TIME_ZONE.defaultValue();
        this.startupOptions = StartupOptions.initial();
        this.splitSize = (Integer) MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.defaultValue();
        this.splitMetaGroupSize = (Integer) MySqlSourceOptions.CHUNK_META_GROUP_SIZE.defaultValue();
        this.fetchSize = (Integer) MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE.defaultValue();
        this.connectTimeout = (Duration) MySqlSourceOptions.CONNECT_TIMEOUT.defaultValue();
        this.connectMaxRetries = (Integer) MySqlSourceOptions.CONNECT_MAX_RETRIES.defaultValue();
        this.connectionPoolSize = (Integer) MySqlSourceOptions.CONNECTION_POOL_SIZE.defaultValue();
        this.distributionFactorUpper = (Double) MySqlSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.defaultValue();
        this.distributionFactorLower = (Double) MySqlSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.defaultValue();
        this.includeSchemaChanges = false;
    }

    public MySqlSourceConfigFactory hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public MySqlSourceConfigFactory port(int port) {
        this.port = port;
        return this;
    }

    public MySqlSourceConfigFactory databaseList(String... databaseList) {
        this.databaseList = Arrays.asList(databaseList);
        return this;
    }

    public MySqlSourceConfigFactory tableList(String... tableList) {
        this.tableList = Arrays.asList(tableList);
        return this;
    }

    public MySqlSourceConfigFactory username(String username) {
        this.username = username;
        return this;
    }

    public MySqlSourceConfigFactory password(String password) {
        this.password = password;
        return this;
    }

    public MySqlSourceConfigFactory serverId(String serverId) {
        this.serverIdRange = ServerIdRange.from(serverId);
        return this;
    }

    public MySqlSourceConfigFactory serverTimeZone(String timeZone) {
        this.serverTimeZone = timeZone;
        return this;
    }

    public MySqlSourceConfigFactory splitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }

    public MySqlSourceConfigFactory splitMetaGroupSize(int splitMetaGroupSize) {
        this.splitMetaGroupSize = splitMetaGroupSize;
        return this;
    }

    public MySqlSourceConfigFactory distributionFactorUpper(double distributionFactorUpper) {
        this.distributionFactorUpper = distributionFactorUpper;
        return this;
    }

    public MySqlSourceConfigFactory distributionFactorLower(double distributionFactorLower) {
        this.distributionFactorLower = distributionFactorLower;
        return this;
    }

    public MySqlSourceConfigFactory fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public MySqlSourceConfigFactory connectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public MySqlSourceConfigFactory connectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    public MySqlSourceConfigFactory connectMaxRetries(int connectMaxRetries) {
        this.connectMaxRetries = connectMaxRetries;
        return this;
    }

    public MySqlSourceConfigFactory includeSchemaChanges(boolean includeSchemaChanges) {
        this.includeSchemaChanges = includeSchemaChanges;
        return this;
    }

    public MySqlSourceConfigFactory startupOptions(StartupOptions startupOptions) {
        /**
         * 支持从binlog offset 读取
         */
        this.startupOptions = startupOptions;
        return this;
//        switch(startupOptions.startupMode) {
//        case INITIAL:
//        case LATEST_OFFSET:
//            this.startupOptions = startupOptions;
//            return this;
//        default:
//            throw new UnsupportedOperationException("Unsupported startup mode: " + startupOptions.startupMode);
//        }
    }

    public MySqlSourceConfigFactory debeziumProperties(Properties properties) {
        this.dbzProperties = properties;
        return this;
    }

    public MySqlSourceConfig createConfig(int subtaskId) {
        Properties props = new Properties();
        props.setProperty("database.server.name", "mysql_binlog_source");
        props.setProperty("database.hostname", (String) Preconditions.checkNotNull(this.hostname));
        props.setProperty("database.user", (String) Preconditions.checkNotNull(this.username));
        props.setProperty("database.password", (String) Preconditions.checkNotNull(this.password));
        props.setProperty("database.port", String.valueOf(this.port));
        props.setProperty("database.fetchSize", String.valueOf(this.fetchSize));
        props.setProperty("database.responseBuffering", "adaptive");
        props.setProperty("database.serverTimezone", this.serverTimeZone);
        props.setProperty("database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID().toString() + "_" + subtaskId);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));
        props.setProperty("connect.timeout.ms", String.valueOf(this.connectTimeout.toMillis()));
        props.setProperty("include.schema.changes", String.valueOf(true));
        props.setProperty("offset.flush.interval.ms", String.valueOf(9223372036854775807L));
        props.setProperty("tombstones.on.delete", String.valueOf(false));
        props.put("bigint.unsigned.handling.mode", "precise");
        if (this.serverIdRange != null) {
            int serverId = this.serverIdRange.getServerId(subtaskId);
            props.setProperty("database.server.id", String.valueOf(serverId));
        }

        if (this.databaseList != null) {
            props.setProperty("database.include.list", String.join(",", this.databaseList));
        }

        if (this.tableList != null) {
            props.setProperty("table.include.list", String.join(",", this.tableList));
        }

        if (this.serverTimeZone != null) {
            props.setProperty("database.serverTimezone", this.serverTimeZone);
        }

        if (this.dbzProperties != null) {
            this.dbzProperties.forEach(props::put);
        }

        return new MySqlSourceConfig(this.hostname, this.port, this.username, this.password, this.databaseList, this.tableList, this.serverIdRange, this.startupOptions, this.splitSize, this.splitMetaGroupSize, this.fetchSize, this.serverTimeZone, this.connectTimeout, this.connectMaxRetries, this.connectionPoolSize, this.distributionFactorUpper, this.distributionFactorLower, this.includeSchemaChanges, props);
    }
}

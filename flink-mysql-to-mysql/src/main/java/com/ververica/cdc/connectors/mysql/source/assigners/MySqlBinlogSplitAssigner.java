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

package com.ververica.cdc.connectors.mysql.source.assigners;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.assigners.state.BinlogPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import io.debezium.jdbc.JdbcConnection;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.currentBinlogOffset;

/**
 * A {@link MySqlSplitAssigner} which only read binlog from current binlog position.
 */
public class MySqlBinlogSplitAssigner implements MySqlSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplitAssigner.class);
    private static final String BINLOG_SPLIT_ID = "binlog-split";

    private final MySqlSourceConfig sourceConfig;

    private boolean isBinlogSplitAssigned;

    public MySqlBinlogSplitAssigner(MySqlSourceConfig sourceConfig) {
        this(sourceConfig, false);
    }

    public MySqlBinlogSplitAssigner(
            MySqlSourceConfig sourceConfig, BinlogPendingSplitsState checkpoint) {
        this(sourceConfig, checkpoint.isBinlogSplitAssigned());
    }

    private MySqlBinlogSplitAssigner(
            MySqlSourceConfig sourceConfig, boolean isBinlogSplitAssigned) {
        this.sourceConfig = sourceConfig;
        this.isBinlogSplitAssigned = isBinlogSplitAssigned;
    }

    @Override
    public void open() {
    }

    @Override
    public Optional<MySqlSplit> getNext() {
        if (isBinlogSplitAssigned) {
            return Optional.empty();
        } else {
            isBinlogSplitAssigned = true;
            return Optional.of(createBinlogSplit());
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return false;
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
        // do nothing
    }

    @Override
    public void addSplits(Collection<MySqlSplit> splits) {
        // we don't store the split, but will re-create binlog split later
        isBinlogSplitAssigned = false;
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new BinlogPendingSplitsState(isBinlogSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // nothing to do
    }

    @Override
    public void close() {
    }

    // ------------------------------------------------------------------------------------------

    private MySqlBinlogSplit createBinlogSplit() {
        try (JdbcConnection jdbc = DebeziumUtils.openJdbcConnection(sourceConfig)) {
            /**
             * 支持从binlog offset 读取
             */
            BinlogOffset binlogOffset = currentBinlogOffset(jdbc);
            if (StartupMode.SPECIFIC_OFFSETS.equals(sourceConfig.getStartupOptions().startupMode)) {
                binlogOffset = new BinlogOffset(
                        sourceConfig.getStartupOptions().specificOffsetFile,
                        sourceConfig.getStartupOptions().specificOffsetPos,
                        binlogOffset.getRestartSkipEvents(),
                        binlogOffset.getRestartSkipRows(),
                        binlogOffset.getTimestamp(),
                        binlogOffset.getGtidSet(),
                        binlogOffset.getServerId().intValue());
            }
            return new MySqlBinlogSplit(
                    BINLOG_SPLIT_ID,
                    binlogOffset,
                    BinlogOffset.NO_STOPPING_OFFSET,
                    new ArrayList<>(),
                    new HashMap<>(),
                    0);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Read the binlog offset error", e);
        }
    }
}

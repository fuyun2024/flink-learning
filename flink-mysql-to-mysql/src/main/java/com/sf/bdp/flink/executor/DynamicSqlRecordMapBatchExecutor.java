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

package com.sf.bdp.flink.executor;

import com.sf.bdp.flink.entity.DynamicSqlRecord;
import com.sf.bdp.flink.statement.JdbcStatementBuilder;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * description:
 * ------------------------------------
 * <p>
 * ------------------------------------
 * created by eHui on 2022/1/3
 */
public class DynamicSqlRecordMapBatchExecutor implements JdbcBatchExecutor<DynamicSqlRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicSqlRecordMapBatchExecutor.class);

    private final JdbcStatementBuilder<DynamicSqlRecord> parameterSetter;
    private final Map<String, List<DynamicSqlRecord>> batch;
    private Connection connection;
    private transient PreparedStatement st;


    public DynamicSqlRecordMapBatchExecutor(JdbcStatementBuilder<DynamicSqlRecord> statementBuilder) {
        this.parameterSetter = statementBuilder;
        this.batch = new HashMap<>();
    }


    @Override
    public void addToBatch(DynamicSqlRecord record) {
        if (batch.get(record.getDbTable()) == null) {
            batch.put(record.getDbTable(), new ArrayList<>());
        }
        List list = batch.get(record.getDbTable());
        list.add(record);
    }


    @Override
    public void executeBatch() throws SQLException {
        if (!batch.isEmpty()) {
            try {

                connection.setAutoCommit(false);
                /**
                 * 执行 sql ,在这期间执行的SQL,只有到 connection.commit() 才会提交
                 */
                execute(batch);
                connection.commit();
                connection.setAutoCommit(true);

                // 清空数据
                batch.clear();
            } catch (Exception e) {
                LOG.error("", e);
                // 回滚
                rollback(connection);
                // 回滚完成后，主动抛出异常
                throw new SQLException("更新数据失败");
            }
        }
    }

    public void execute(Map<String, List<DynamicSqlRecord>> batch) throws SQLException {
        for (List<DynamicSqlRecord> recordList : batch.values()) {
            execute(recordList);
        }
    }

    public void execute(List<DynamicSqlRecord> recordList) throws SQLException {
        boolean isUpdate = true;
        RowKind currentMode = null;
        for (DynamicSqlRecord record : recordList) {
            if (!record.getKind().equals(currentMode)) {
                currentMode = record.getKind();
            }

            if (isUpdate) {
                if (st != null) {
                    st.executeBatch();
                }
                st = connection.prepareStatement(record.getSql());
                isUpdate = false;
            }

            parameterSetter.accept(st, record);
            st.addBatch();
        }

        st.executeBatch();
        st = null;
    }

    @Override
    public void resetConnection(Connection connection) {
        this.connection = connection;
    }


    private void rollback(Connection connection) {
        if (connection != null) {
            try {
                LOG.info("正在回滚更新失败的数据");
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
    }

}

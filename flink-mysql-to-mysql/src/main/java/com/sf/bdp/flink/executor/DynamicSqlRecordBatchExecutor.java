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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * description:
 * ------------------------------------
 * <p>
 * ------------------------------------
 * created by eHui on 2022/1/3
 */
public class DynamicSqlRecordBatchExecutor implements JdbcBatchExecutor<DynamicSqlRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicSqlRecordBatchExecutor.class);

    private final JdbcStatementBuilder<DynamicSqlRecord> parameterSetter;
    private final List<DynamicSqlRecord> batch;


    // sql
    private Connection connection;
    private transient PreparedStatement st;

    public DynamicSqlRecordBatchExecutor(JdbcStatementBuilder<DynamicSqlRecord> statementBuilder) {
        this.parameterSetter = statementBuilder;
        this.batch = new ArrayList<>();
    }


    @Override
    public void addToBatch(DynamicSqlRecord record) {
        batch.add(record);
    }


    @Override
    public void executeBatch() throws SQLException {
        if (!batch.isEmpty()) {
            connection.setAutoCommit(false);
            for (DynamicSqlRecord data : batch) {
                execute(data);
            }
            connection.commit();
            batch.clear();
        }
    }


    public void execute(DynamicSqlRecord data) throws SQLException {
        st = connection.prepareStatement(data.getSql());
        parameterSetter.accept(st, data);
        st.execute();
    }

    @Override
    public void resetConnection(Connection connection) {
        this.connection = connection;
    }

}

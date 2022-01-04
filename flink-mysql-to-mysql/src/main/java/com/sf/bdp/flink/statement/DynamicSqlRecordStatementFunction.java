/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sf.bdp.flink.statement;

import com.sf.bdp.flink.entity.DynamicSqlRecord;
import com.sf.bdp.flink.utils.JdbcTypeUtil;
import org.apache.kafka.connect.data.Schema;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;


/**
 * description:
 * ------------------------------------
 * <p>
 * ------------------------------------
 * created by eHui on 2021/12/31
 */
public class DynamicSqlRecordStatementFunction implements JdbcStatementBuilder<DynamicSqlRecord> {

    @Override
    public void accept(PreparedStatement preparedStatement, DynamicSqlRecord dynamicRowData) throws SQLException {
        Schema.Type type;
        Object value;
        JdbcSerializationConverter converter;

        for (int i = 0; i < dynamicRowData.getFieldNames().length; i++) {
            type = dynamicRowData.getFieldTypes()[i];
            value = dynamicRowData.getValues()[i];

            converter = createObjectExternalConverter(type);
            converter.serialize(Optional.ofNullable(value), i + 1, preparedStatement);
        }
    }

    @FunctionalInterface
    interface JdbcSerializationConverter extends Serializable {
        void serialize(Optional<Object> var, int index, PreparedStatement statement)
                throws SQLException;
    }


    protected JdbcSerializationConverter createObjectExternalConverter(Schema.Type type) {
        final int sqlType =
                JdbcTypeUtil.schemaTypeToSqlType(type);

        return (val, index, statement) -> {
            if (!val.isPresent()) {
                statement.setNull(index, sqlType);
            } else {
                statement.setObject(index, val.get());
            }
        };
    }

    protected JdbcSerializationConverter createNullableExternalConverter(Schema.Type type) {
        return wrapIntoNullableExternalConverter(createExternalConverter(type), type);
    }


    protected JdbcSerializationConverter wrapIntoNullableExternalConverter(
            JdbcSerializationConverter jdbcSerializationConverter, Schema.Type type) {
        final int sqlType =
                JdbcTypeUtil.schemaTypeToSqlType(type);

        return (val, index, statement) -> {
            if (!val.isPresent()) {
                statement.setNull(index, sqlType);
            } else {
                jdbcSerializationConverter.serialize(val, index, statement);
            }
        };
    }


    private JdbcSerializationConverter createExternalConverter(Schema.Type type) {
        switch (type) {
            case BOOLEAN:
                return (val, index, statement) -> statement.setBoolean(index, (boolean) val.get());
            case INT8:
                return (val, index, statement) -> statement.setByte(index, (byte) val.get());
            case INT16:
                return (val, index, statement) -> statement.setShort(index, (short) val.get());
            case INT32:
                return (val, index, statement) -> statement.setInt(index, (int) val.get());
            case INT64:
                return (val, index, statement) -> statement.setLong(index, (long) val.get());
            case FLOAT32:
                return (val, index, statement) -> statement.setFloat(index, (float) val.get());
            case FLOAT64:
                return (val, index, statement) -> statement.setDouble(index, (double) val.get());
            case STRING:
                return (val, index, statement) -> statement.setString(index, val.get().toString());
            case BYTES:
                return (val, index, statement) -> {
                    if (val.get() instanceof byte[]) {
                        statement.setBytes(index, (byte[]) val.get());
                    } else {
                        statement.setByte(index, (byte) val.get());
                    }
                };
            case ARRAY:
            case MAP:
            case STRUCT:
                return (val, index, statement) -> statement.setObject(index, val.get());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

}

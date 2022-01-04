package com.sf.bdp.flink.entity;

import org.apache.flink.types.RowKind;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;

/**
 * description:
 * ------------------------------------
 * <p>
 * ------------------------------------
 * created by eHui on 2021/12/31
 */
public class DynamicSqlRecord {

    private final String dbTable;
    private final RowKind kind;
    private final String sql;
    private final String[] fieldNames;
    private final Schema.Type[] fieldTypes;
    private final Object[] values;


    public DynamicSqlRecord(String dbTable, RowKind kind, String sql, String[] fieldNames, Schema.Type[] fieldTypes, Object[] values) {
        this.dbTable = dbTable;
        this.kind = kind;
        this.sql = sql;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.values = values;
    }

    public String getDbTable() {
        return dbTable;
    }

    public RowKind getKind() {
        return kind;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public Schema.Type[] getFieldTypes() {
        return fieldTypes;
    }

    public Object[] getValues() {
        return values;
    }

    public String getSql() {
        return sql;
    }


    @Override
    public String toString() {
        return "DynamicSqlRecord{" +
                "dbTable='" + dbTable + '\'' +
                ", kind=" + kind +
                ", sql='" + sql + '\'' +
                ", fieldNames=" + Arrays.toString(fieldNames) +
                ", fieldTypes=" + Arrays.toString(fieldTypes) +
                ", values=" + Arrays.toString(values) +
                '}';
    }
}


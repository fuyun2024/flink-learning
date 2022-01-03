package com.sf.bdp.flink.entity;

import org.apache.kafka.connect.data.Schema;

/**
 * description:
 * ------------------------------------
 * <p>
 * ------------------------------------
 * created by eHui on 2021/12/31
 */
public class DynamicSqlRecord {

    private final String sql;
    private final String[] fieldNames;
    private final Schema.Type[] fieldTypes;
    private final Object[] values;


    public DynamicSqlRecord(String sql, String[] fieldNames, Schema.Type[] fieldTypes, Object[] values) {
        this.sql = sql;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.values = values;
    }

    public String getSql() {
        return sql;
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

    public static DynamicSqlRecord forDynamicRowData(DynamicRowRecord rowData, String executeSql) {
        return new DynamicSqlRecord(executeSql, rowData.getFieldNames(), rowData.getFieldTypes(), rowData.getValues());
    }


}


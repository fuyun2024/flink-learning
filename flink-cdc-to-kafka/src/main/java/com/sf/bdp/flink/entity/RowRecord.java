package com.sf.bdp.flink.entity;

import com.alibaba.fastjson.JSON;
import org.apache.flink.types.RowKind;
import org.apache.kafka.connect.data.Schema;


/**
 * description:
 * ------------------------------------
 * <p>
 * ------------------------------------
 * created by eHui on 2021/12/31
 */
public class RowRecord {

    private String dbName;
    private String tableName;
    private RowKind kind;

    private String[] keyNames;
    private Schema.Type[] keyTypes;
    private Object[] keyValues;

    private String[] fieldNames;
    private Schema.Type[] fieldTypes;
    private Object[] values;


    public static RowRecord buildUpsertRecord(String dbName, String tableName, RowKind kind, String[] fieldNames, Schema.Type[] fieldTypes, Object[] values) {
        if (RowKind.DELETE.equals(kind)) {
            throw new UnsupportedOperationException("build upsert record not supper type:" + RowKind.DELETE);
        }
        RowRecord upsertRecord = new RowRecord();
        upsertRecord.setDbName(dbName);
        upsertRecord.setTableName(tableName);
        upsertRecord.setKind(kind);
        upsertRecord.setFieldNames(fieldNames);
        upsertRecord.setFieldTypes(fieldTypes);
        upsertRecord.setValues(values);
        return upsertRecord;
    }


    public static RowRecord buildAllRecord(String dbName, String tableName, RowKind kind,
                                           String[] keyNames, Schema.Type[] keyTypes, Object[] keyValues,
                                           String[] fieldNames, Schema.Type[] fieldTypes, Object[] values) {
          RowRecord record = new RowRecord();
        record.setDbName(dbName);
        record.setTableName(tableName);
        record.setKind(kind);
        record.setKeyNames(keyNames);
        record.setKeyTypes(keyTypes);
        record.setKeyValues(keyValues);
        record.setFieldNames(fieldNames);
        record.setFieldTypes(fieldTypes);
        record.setValues(values);
        return record;
    }


    public static RowRecord buildDeletedRecord(String dbName, String tableName, RowKind kind, String[] keyNames, Schema.Type[] keyTypes, Object[] keyValues) {
        if (!RowKind.DELETE.equals(kind)) {
            throw new UnsupportedOperationException("build delete record type:" + RowKind.DELETE);
        }
        RowRecord deleteRecord = new RowRecord();
        deleteRecord.setDbName(dbName);
        deleteRecord.setTableName(tableName);
        deleteRecord.setKind(kind);
        deleteRecord.setKeyNames(keyNames);
        deleteRecord.setKeyTypes(keyTypes);
        deleteRecord.setKeyValues(keyValues);
        return deleteRecord;
    }


    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public RowKind getKind() {
        return kind;
    }

    public void setKind(RowKind kind) {
        this.kind = kind;
    }

    public String[] getKeyNames() {
        return keyNames;
    }

    public void setKeyNames(String[] keyNames) {
        this.keyNames = keyNames;
    }

    public Schema.Type[] getKeyTypes() {
        return keyTypes;
    }

    public void setKeyTypes(Schema.Type[] keyTypes) {
        this.keyTypes = keyTypes;
    }

    public Object[] getKeyValues() {
        return keyValues;
    }

    public void setKeyValues(Object[] keyValues) {
        this.keyValues = keyValues;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public Schema.Type[] getFieldTypes() {
        return fieldTypes;
    }

    public void setFieldTypes(Schema.Type[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public Object[] getValues() {
        return values;
    }

    public void setValues(Object[] values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}


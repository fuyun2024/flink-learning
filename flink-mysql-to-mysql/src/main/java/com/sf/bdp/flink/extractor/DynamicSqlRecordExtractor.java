package com.sf.bdp.flink.extractor;

import com.sf.bdp.flink.entity.DynamicRowRecord;
import com.sf.bdp.flink.entity.DynamicSqlRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class DynamicSqlRecordExtractor implements RecordExtractor<Tuple2<String, DynamicRowRecord>, DynamicSqlRecord> {


    @Override
    public DynamicSqlRecord apply(Tuple2<String, DynamicRowRecord> tuple2) {
        DynamicRowRecord record = tuple2.f1;
        switch (record.getKind()) {
            case INSERT:
            case UPDATE_BEFORE:
            case UPDATE_AFTER:
                String upsertSql = getUpsertSql(record.getDbName(), record.getTableName(), record.getFieldNames());
                return new DynamicSqlRecord(upsertSql, RowKind.INSERT, upsertSql,
                        record.getFieldNames(), record.getFieldTypes(), record.getValues());
            case DELETE:
                String deleteSql = getDeleteSql(record.getDbName(), record.getTableName(), record.getKeyNames());
                return new DynamicSqlRecord(deleteSql, RowKind.DELETE, deleteSql,
                        record.getKeyNames(), record.getKeyTypes(), record.getKeyValues());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + record.getKind());
        }
    }


    /**
     * 生成 upsert 语句
     *
     * @param dbName
     * @param tableName
     * @param fieldNames
     * @return
     */
    public String getUpsertSql(
            String dbName, String tableName, String[] fieldNames) {
        String updateClause =
                Arrays.stream(fieldNames)
                        .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                        .collect(Collectors.joining(", "));
        return getInsertIntoSql(dbName, tableName, fieldNames)
                + " ON DUPLICATE KEY UPDATE "
                + updateClause;
    }


    String getInsertIntoSql(String dbName, String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames).map(f -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO "
                + quoteIdentifier(dbName) + "." + quoteIdentifier(tableName)
                + "("
                + columns
                + ")"
                + " VALUES ("
                + placeholders
                + ")";
    }


    /**
     * 生成 delete 语句
     *
     * @param dbName
     * @param tableName
     * @param conditionFields
     * @return
     */
    String getDeleteSql(String dbName, String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = ?", quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM " + quoteIdentifier(dbName) + "." + quoteIdentifier(tableName) + " WHERE " + conditionClause;
    }


    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }


}

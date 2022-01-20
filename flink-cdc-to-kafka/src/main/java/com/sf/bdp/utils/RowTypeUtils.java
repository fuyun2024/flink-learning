package com.sf.bdp.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.List;

public class RowTypeUtils {


    /**
     * 构建 Debezium 的 RowType
     *
     * @param dataType
     * @return
     */
    public static RowType createDebeziumAvroRowType(DataType dataType) {
        return (RowType)
                DataTypes.ROW(
                        DataTypes.FIELD("before", dataType.nullable()),
                        DataTypes.FIELD("after", dataType.nullable()),
                        DataTypes.FIELD("op", DataTypes.STRING()))
                        .getLogicalType();
    }

    /**
     * 构架数据的 RowType
     *
     * @param fieldNames
     * @param fieldTypes
     * @return
     */
    public static RowType createRowType(String[] fieldNames, Schema.Type[] fieldTypes) {
        List<RowType.RowField> fields = new ArrayList<>();
        for (int i = 0; i < fieldNames.length; i++) {
            fields.add(RowTypeUtils.convertRowField(fieldNames[i], fieldTypes[i]));
        }

        return new RowType(fields);
    }


    /**
     * 数据类型转换 :  mysql cdc rowType -> flink rowType
     *
     * @param fieldName
     * @param type
     * @return
     */
    public static RowType.RowField convertRowField(String fieldName, org.apache.kafka.connect.data.Schema.Type type) {
        switch (type) {
            case BOOLEAN:
                return new RowType.RowField(fieldName, new BooleanType());
            case INT8:
                return new RowType.RowField(fieldName, new TinyIntType());
            case INT16:
                return new RowType.RowField(fieldName, new SmallIntType());
            case INT32:
                return new RowType.RowField(fieldName, new IntType());
            case INT64:
                return new RowType.RowField(fieldName, new BigIntType());
            case FLOAT32:
                return new RowType.RowField(fieldName, new FloatType());
            case FLOAT64:
                return new RowType.RowField(fieldName, new DoubleType());
            case STRING:
                return new RowType.RowField(fieldName, new VarCharType());
            case BYTES:
                return new RowType.RowField(fieldName, new BinaryType());
            case ARRAY:
            case MAP:
            case STRUCT:
                // todo 数据类型映射估计还是有问题的
                return new RowType.RowField(fieldName, new BinaryType());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }


}

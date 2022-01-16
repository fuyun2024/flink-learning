package com.sf.bdp.utils;

import com.sf.bdp.entity.GenericRowRecord;
import org.apache.flink.table.types.logical.*;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.List;

public class RowTypeUtils {



    public static RowType getRowType(GenericRowRecord record) {
        String[] fieldNames = record.getFieldNames();
        Schema.Type[] fieldTypes = record.getFieldTypes();

        List<RowType.RowField> fields = new ArrayList<>();
        for (int i = 0; i < fieldNames.length; i++) {
            fields.add(RowTypeUtils.convertRowField(fieldNames[i], fieldTypes[i]));
        }

        return new RowType(fields);
    }


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
                return new RowType.RowField(fieldName, new BinaryType());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }


}

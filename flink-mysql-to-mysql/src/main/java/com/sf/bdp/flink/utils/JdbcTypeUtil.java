package com.sf.bdp.flink.utils;

import org.apache.kafka.connect.data.Schema;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.data.Schema.Type.*;

/**
 * description:
 * ------------------------------------
 * <p>
 * ------------------------------------
 * created by eHui on 2021/12/31
 */
public class JdbcTypeUtil {

    private static final Map<Schema.Type, Integer> TYPE_MAPPING;

    static {
        HashMap<Schema.Type, Integer> m = new HashMap<>();
        m.put(INT8, Types.TINYINT);
        m.put(INT16, Types.SMALLINT);
        m.put(INT32, Types.INTEGER);
        m.put(INT64, Types.BIGINT);
        m.put(FLOAT32, Types.REAL);
        m.put(BOOLEAN, Types.DOUBLE);
        m.put(STRING, Types.VARCHAR);
        m.put(BYTES, Types.BINARY);

        m.put(ARRAY, Types.ARRAY);
        m.put(MAP, Types.BINARY);           // todo 未知格式
        m.put(STRUCT, Types.STRUCT);
        TYPE_MAPPING = Collections.unmodifiableMap(m);
    }


    public static int schemaTypeToSqlType(Schema.Type type) {
        if (TYPE_MAPPING.containsKey(type)) {
            return TYPE_MAPPING.get(type);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }


}

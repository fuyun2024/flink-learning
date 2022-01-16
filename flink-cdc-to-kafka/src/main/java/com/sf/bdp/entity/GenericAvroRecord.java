package com.sf.bdp.entity;

import com.alibaba.fastjson.JSON;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;


/**
 * description:
 * ------------------------------------
 * <p>
 * ------------------------------------
 * created by eHui on 2021/12/31
 */
public class GenericAvroRecord implements Serializable {

    private String topicName;
    private Schema schema;
    private GenericRecord genericRecord;

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public GenericRecord getGenericRecord() {
        return genericRecord;
    }

    public void setGenericRecord(GenericRecord genericRecord) {
        this.genericRecord = genericRecord;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}


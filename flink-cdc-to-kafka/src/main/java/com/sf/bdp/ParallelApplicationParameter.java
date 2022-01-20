package com.sf.bdp;

import com.alibaba.fastjson.JSON;
import com.sf.bdp.entity.DatabaseInfoBean;

import java.util.List;

public class ParallelApplicationParameter {


    /**
     * source 参数
     */
    List<DatabaseInfoBean> databaseInfoBeanList;


    /**
     * sink 参数
     */
    private String sinkBootstrapServers;
    private String sinkGroupId;
    private String sinkEnableAutoCommit;

    /**
     * dbTableTopicMap
     */
    private String dbTableTopicMap;


    /**
     * flink checkPont
     */
    private String checkpointInterval;
    private String checkpointingMode;
    private String checkpointDir;

    public List<DatabaseInfoBean> getDatabaseInfoBeanList() {
        return databaseInfoBeanList;
    }

    public void setDatabaseInfoBeanList(List<DatabaseInfoBean> databaseInfoBeanList) {
        this.databaseInfoBeanList = databaseInfoBeanList;
    }

    public String getSinkBootstrapServers() {
        return sinkBootstrapServers;
    }

    public void setSinkBootstrapServers(String sinkBootstrapServers) {
        this.sinkBootstrapServers = sinkBootstrapServers;
    }

    public String getSinkGroupId() {
        return sinkGroupId;
    }

    public void setSinkGroupId(String sinkGroupId) {
        this.sinkGroupId = sinkGroupId;
    }

    public String getSinkEnableAutoCommit() {
        return sinkEnableAutoCommit;
    }

    public void setSinkEnableAutoCommit(String sinkEnableAutoCommit) {
        this.sinkEnableAutoCommit = sinkEnableAutoCommit;
    }

    public String getDbTableTopicMap() {
        return dbTableTopicMap;
    }

    public void setDbTableTopicMap(String dbTableTopicMap) {
        this.dbTableTopicMap = dbTableTopicMap;
    }

    public String getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(String checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    public String getCheckpointingMode() {
        return checkpointingMode;
    }

    public void setCheckpointingMode(String checkpointingMode) {
        this.checkpointingMode = checkpointingMode;
    }

    public String getCheckpointDir() {
        return checkpointDir;
    }

    public void setCheckpointDir(String checkpointDir) {
        this.checkpointDir = checkpointDir;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}

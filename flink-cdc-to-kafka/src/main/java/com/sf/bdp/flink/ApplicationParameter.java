package com.sf.bdp.flink;

import com.alibaba.fastjson.JSON;

public class ApplicationParameter {

    /**
     * source 参数
     */
    private String sourceHostName;
    private String sourcePort;
    private String sourceDatabaseList;
    private String sourceTableList;
    private String sourceUsername;
    private String sourcePassword;

    /**
     * 启动位置信息
     */
    private String specificOffsetFile;
    private String specificOffsetPos;

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

    public String getSourceHostName() {
        return sourceHostName;
    }

    public void setSourceHostName(String sourceHostName) {
        this.sourceHostName = sourceHostName;
    }

    public String getSourcePort() {
        return sourcePort;
    }

    public void setSourcePort(String sourcePort) {
        this.sourcePort = sourcePort;
    }

    public String getSourceDatabaseList() {
        return sourceDatabaseList;
    }

    public void setSourceDatabaseList(String sourceDatabaseList) {
        this.sourceDatabaseList = sourceDatabaseList;
    }

    public String getSourceTableList() {
        return sourceTableList;
    }

    public void setSourceTableList(String sourceTableList) {
        this.sourceTableList = sourceTableList;
    }

    public String getSourceUsername() {
        return sourceUsername;
    }

    public void setSourceUsername(String sourceUsername) {
        this.sourceUsername = sourceUsername;
    }

    public String getSourcePassword() {
        return sourcePassword;
    }

    public void setSourcePassword(String sourcePassword) {
        this.sourcePassword = sourcePassword;
    }

    public String getSpecificOffsetFile() {
        return specificOffsetFile;
    }

    public void setSpecificOffsetFile(String specificOffsetFile) {
        this.specificOffsetFile = specificOffsetFile;
    }

    public String getSpecificOffsetPos() {
        return specificOffsetPos;
    }

    public void setSpecificOffsetPos(String specificOffsetPos) {
        this.specificOffsetPos = specificOffsetPos;
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

    public String getDbTableTopicMap() {
        return dbTableTopicMap;
    }

    public void setDbTableTopicMap(String dbTableTopicMap) {
        this.dbTableTopicMap = dbTableTopicMap;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}

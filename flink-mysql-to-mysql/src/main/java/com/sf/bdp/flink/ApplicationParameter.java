package com.sf.bdp.flink;

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
    private String driverName;
    private String sinkJdbcUrl;
    private String sinkUsername;
    private String sinkPassword;

    /**
     * sink 配置
     */
    private String sinkFlushMaxSize;
    private String sinkMaxRetryTimes;
    private String sinkFlushIntervalMills;


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

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getSinkJdbcUrl() {
        return sinkJdbcUrl;
    }

    public void setSinkJdbcUrl(String sinkJdbcUrl) {
        this.sinkJdbcUrl = sinkJdbcUrl;
    }

    public String getSinkUsername() {
        return sinkUsername;
    }

    public void setSinkUsername(String sinkUsername) {
        this.sinkUsername = sinkUsername;
    }

    public String getSinkPassword() {
        return sinkPassword;
    }

    public void setSinkPassword(String sinkPassword) {
        this.sinkPassword = sinkPassword;
    }

    public String getSinkFlushMaxSize() {
        return sinkFlushMaxSize;
    }

    public void setSinkFlushMaxSize(String sinkFlushMaxSize) {
        this.sinkFlushMaxSize = sinkFlushMaxSize;
    }

    public String getSinkMaxRetryTimes() {
        return sinkMaxRetryTimes;
    }

    public void setSinkMaxRetryTimes(String sinkMaxRetryTimes) {
        this.sinkMaxRetryTimes = sinkMaxRetryTimes;
    }

    public String getSinkFlushIntervalMills() {
        return sinkFlushIntervalMills;
    }

    public void setSinkFlushIntervalMills(String sinkFlushIntervalMills) {
        this.sinkFlushIntervalMills = sinkFlushIntervalMills;
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
        return "ApplicationParameter{" +
                "sourceHostName='" + sourceHostName + '\'' +
                ", sourcePort='" + sourcePort + '\'' +
                ", sourceDatabaseList='" + sourceDatabaseList + '\'' +
                ", sourceTableList='" + sourceTableList + '\'' +
                ", sourceUsername='" + sourceUsername + '\'' +
                ", sourcePassword='" + sourcePassword + '\'' +
                ", specificOffsetFile='" + specificOffsetFile + '\'' +
                ", specificOffsetPos='" + specificOffsetPos + '\'' +
                ", driverName='" + driverName + '\'' +
                ", sinkJdbcUrl='" + sinkJdbcUrl + '\'' +
                ", sinkUsername='" + sinkUsername + '\'' +
                ", sinkPassword='" + sinkPassword + '\'' +
                ", sinkFlushMaxSize='" + sinkFlushMaxSize + '\'' +
                ", sinkMaxRetryTimes='" + sinkMaxRetryTimes + '\'' +
                ", sinkFlushIntervalMills='" + sinkFlushIntervalMills + '\'' +
                ", checkpointInterval='" + checkpointInterval + '\'' +
                ", checkpointingMode='" + checkpointingMode + '\'' +
                ", checkpointDir='" + checkpointDir + '\'' +
                '}';
    }
}

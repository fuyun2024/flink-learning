package com.sf.bdp.entity;

public class DatabaseInfoBean {

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
}

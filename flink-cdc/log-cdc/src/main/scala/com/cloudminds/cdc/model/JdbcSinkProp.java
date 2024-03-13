package com.cloudminds.cdc.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class JdbcSinkProp implements Serializable {
    /**
     * jdbcDriver 驱动
     */
    private String jdbcDriver = "";
    /**
     * jdbcUrl 链接地址
     */
    private String jdbcUrl = "";
    /**
     * username 用户名
     */
    private String username = "";
    /**
     * password 密码
     */
    private String password = "";
    /**
     * batchSize 一批次提交数量
     */
    private int batchSize = 100;
    /**
     * maxRetries 重试次数
     */
    private int maxRetries =3;
    /**
     * batchInterval 等待时间 即使没达到对应的批次也会提交
     */
    private int batchInterval = 6000;

    public JdbcSinkProp(Builder build){
        this.jdbcDriver = build.jdbcDriver;
        this.jdbcUrl = build.jdbcUrl;
        this.username = build.username;
        this.password = build.password;
        this.batchSize = build.batchSize;
        this.maxRetries = build.maxRetries;
        this.batchInterval = build.batchInterval;
    }

    public static class Builder {
        private String jdbcDriver = "";
        private String jdbcUrl = "";
        private String username = "";
        private String password = "";
        private int batchSize = 100;
        private int maxRetries =3;
        private int batchInterval = 6000;
        public Builder setJdbcDriver(String jdbcDriver){
            this.jdbcDriver = jdbcDriver;
            return this;
        }
        public Builder setJdbcUrl(String jdbcUrl){
            this.jdbcUrl = jdbcUrl;
            return this;
        }
        public Builder setUsername(String username){
            this.username = username;
            return this;
        }
        public Builder setPassword(String password){
            this.password = password;
            return this;
        }
        public Builder setBatchSize(Integer batchSize){
            this.batchSize = batchSize;
            return this;
        }
        public Builder setMaxRetries(Integer maxRetries){
            this.maxRetries = maxRetries;
            return this;
        }
        public Builder setBatchInterval(Integer batchInterval){
            this.batchInterval = batchInterval;
            return this;
        }
        public Builder(){

        };
        public JdbcSinkProp build(){
            return new JdbcSinkProp(this);
        }
    }
}

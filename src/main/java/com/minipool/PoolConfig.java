package com.minipool;

public class PoolConfig {
    private String jdbcUrl;
    private String username;
    private String password;
    private int initialSize = 5;
    private int maxPoolSize = 10;
    private int minIdle = 5;                      // 最小空闲连接数
    private long maxIdleTime = 5000L;           // 空闲连接最大存活时间（ms）默认5分钟 300_000L
    private long borrowTimeout = 3000L;
    private long leakDetectionInterval = 1L; // 泄漏检测间隔（秒）默认10秒
    private long leakDetectionThreshold = 10000L; // 泄漏告警阈值（ms）默认60秒
    private long leakForceCloseThreshold = 20000L;// 泄漏强制关闭阈值（ms）默认120秒
    private long idleEvictionInterval = 3L;       // 空闲回收检查间隔（秒）默认30秒


    public PoolConfig(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }
    public PoolConfig(String jdbcUrl, String username, String password, int initialSize, int maxPoolSize, long borrowTimeout) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.initialSize = initialSize;
        this.maxPoolSize = maxPoolSize;
        this.borrowTimeout = borrowTimeout;
    }
    // 空闲回收检查间隔（秒）默认30秒
    public long getIdleEvictionInterval() {
        return idleEvictionInterval;
    }
    public void setIdleEvictionInterval(long idleEvictionInterval) {
        this.idleEvictionInterval = idleEvictionInterval;
    }
    // 空闲连接最大存活时间（ms）默认5分钟
    public long getMaxIdleTime() {
        return maxIdleTime;
    }
    public void setMaxIdleTime(long maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }
    // 最小空闲连接数默认5
    public int getMinIdle() {
        return minIdle;
    }
    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }
    public String getJdbcUrl() {
        return jdbcUrl;
    }
    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getInitialSize() {
        return initialSize;
    }

    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public long getBorrowTimeout() {
        return borrowTimeout;
    }

    public void setBorrowTimeout(long borrowTimeout) {
        this.borrowTimeout = borrowTimeout;
    }

    public long getLeakDetectionInterval() {
        return leakDetectionInterval;
    }

    public long getLeakDetectionThreshold() {
        return leakDetectionThreshold;
    }

    public long getLeakForceCloseThreshold() {
        return leakForceCloseThreshold;
    }
}

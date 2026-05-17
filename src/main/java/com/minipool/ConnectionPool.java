
package com.minipool;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Data;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class ConnectionPool {
    private final Deque<Connection> idleQueue = new ArrayDeque<>();
    private final Set<Connection> activeSet = new HashSet<>();
    private PoolConfig config;
    private String jdbcUrl;
    private String username;
    private String password;
    private ReentrantLock lock=new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final AtomicInteger totalCount = new AtomicInteger(0);
    private PoolMetrics poolMetrics=new PoolMetrics();
    private final ConcurrentHashMap<Connection, Long> returnTimeMap = new ConcurrentHashMap<>();
    // ConnectionPool 新增后台线程
    private final ScheduledExecutorService scheduler;
    public ConnectionPool(PoolConfig config, String jdbcUrl, String username, String password) throws SQLException {
        this.config = config;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.lock = new ReentrantLock();
        this.scheduler = Executors.newScheduledThreadPool(2);
        
        startLeakDetection();   
        startIdleEviction();    
        initPool();
    }
    private void initPool() throws SQLException {
        for (int i = 0; i < config.getInitialSize(); i++) {
            idleQueue.offer(createRealConnection());
            totalCount.incrementAndGet();
        }
    }

    public ConnectionPool(PoolConfig config) throws SQLException {
        this.config = config;
        this.scheduler = Executors.newScheduledThreadPool(2);
                
        startLeakDetection();   
        startIdleEviction();    
        initPool();
    }

    public Connection borrowConnection() throws SQLException {
        long borrowStart = System.currentTimeMillis();
        lock.lock();
        try{
            System.out.println("idleQueue.isEmpty()=" + idleQueue.isEmpty()   + ", totalCount=" + totalCount.get());
            
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getBorrowTimeout());
            while(idleQueue.isEmpty() && System.nanoTime() < deadline){
                if(totalCount.get() < config.getMaxPoolSize() && idleQueue.isEmpty()){
                    totalCount.incrementAndGet();
                    lock.unlock(); 
                    try{
                        Connection con = createRealConnection();
                        lock.lock();
                        idleQueue.offer(con);
                        notEmpty.signal();
                        break;
                    }catch(SQLException e){
                        totalCount.decrementAndGet();
                        e.printStackTrace();
                        lock.lock();
                    }
                }
                long remaining = deadline - System.nanoTime();
                if(remaining <= 0){
                    throw new SQLException(
                    "Connection borrow timeout after " + config.getBorrowTimeout() + "ms" +
                    " [active=" + activeSet.size() + ", idle=" + idleQueue.size() + "]"
                    );
                }
                notEmpty.awaitNanos(remaining);
            }
            Connection con = idleQueue.poll();
            if (con == null) {
                throw new SQLException(
                    "Connection borrow timeout after " + config.getBorrowTimeout() + "ms" +
                    " [active=" + activeSet.size() + ", idle=" + idleQueue.size() + "]"
                );
            }
            returnTimeMap.remove(con);
            Connection proxy = PooledConnection.wrap(con, this);
            PooledConnection handler = (PooledConnection) Proxy.getInvocationHandler(proxy);
            handler.setBorrowTime(System.currentTimeMillis());
            activeSet.add(proxy);

            long borrowDuration = System.currentTimeMillis() - borrowStart;
            poolMetrics.getBorrowCount().increment();
            poolMetrics.getTotalBorrowTime().add(borrowDuration);
            if (borrowDuration > poolMetrics.getMaxBorrowTime()) {
                poolMetrics.setMaxBorrowTime(borrowDuration);
            }
            long totalBorrows = poolMetrics.getBorrowCount().sum();
            if (totalBorrows > 0) {
                poolMetrics.setAvgBorrowTime(poolMetrics.getTotalBorrowTime().sum() / totalBorrows);
            }
            updateRealtimeMetrics();

            return proxy;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally{
            lock.unlock();
        }
    }
    public void releaseConnection(Connection proxy){
       lock.lock();
       try {
            activeSet.remove(proxy);
            updateRealtimeMetrics();
       } finally{
            lock.unlock();
       }
        PooledConnection handler = (PooledConnection) Proxy.getInvocationHandler(proxy);
        poolMetrics.getReturnCount().increment();
        Connection real = PooledConnection.unwrap(proxy);
        if(isConnectionAlive(real)){
            returnTimeMap.put(real, System.currentTimeMillis());
            System.out.println("连接归还! proxy=" + proxy + ",时间=" + System.currentTimeMillis());
            lock.lock();
            try {
                idleQueue.offer(real);
                notEmpty.signal();
                updateRealtimeMetrics();
            } finally {
                lock.unlock();
            }
        }else{
            closeQuietly(real);
            totalCount.decrementAndGet();
            lock.lock();
            try {
                notEmpty.signal();
                updateRealtimeMetrics();
            } finally {
                lock.unlock();
            }
        }
        
    }
    private Connection createRealConnection() throws SQLException {
        return DriverManager.getConnection(
            config.getJdbcUrl(),
            config.getUsername(),
            config.getPassword()
        );
    }

    public void shutdown() throws SQLException {
        scheduler.shutdownNow();
        lock.lock();
        try {
            for (Connection conn : idleQueue) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
            idleQueue.clear();
            returnTimeMap.clear();
            for (Connection proxy : activeSet) {
                try {
                    Connection real = PooledConnection.unwrap(proxy);
                    real.close();
                } catch (SQLException e) {
                }
            }
            activeSet.clear();
        } finally {
            lock.unlock();
        }
    }
    // 在 ConnectionPool 里加这几个方法
    public int getIdleCount()   { return idleQueue.size(); }
    public int getActiveCount() { return activeSet.size(); }
    public int getTotalCount()  { return totalCount.get(); }
    public int getWaitingCount(){ return lock.getQueueLength(); }
    public PoolMetrics getPoolMetrics() { return poolMetrics; }

    private void updateRealtimeMetrics() {
        poolMetrics.getActiveCount().reset();
        poolMetrics.getActiveCount().add(activeSet.size());
        poolMetrics.getIdleCount().reset();
        poolMetrics.getIdleCount().add(idleQueue.size());
        poolMetrics.getTotalCount().reset();
        poolMetrics.getTotalCount().add(totalCount.get());
    }

    private boolean isConnectionAlive(Connection conn) {
        try {
            return !conn.isClosed() && conn.isValid(1);
        } catch (SQLException e) {
            return false;
        }
    }
    private void closeQuietly(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }


    // 连接泄漏检测。  要注意并发问题，在遍历过程中可能发生归还借出。
    //直接加锁会导致阻塞所有线程，串行化
    //采用复制快照，无锁检查
    void startLeakDetection() {
        scheduler.scheduleAtFixedRate(() -> {
            //System.out.println("leakDetectionInterval=" + config.getLeakDetectionInterval() + ", leakDetectionThreshold=" + config.getLeakDetectionThreshold() + ", leakForceCloseThreshold=" + config.getLeakForceCloseThreshold());
            long now = System.currentTimeMillis();

            //复制快照短暂加锁，确保在遍历过程中 activeSet 不会被修改
            List<Connection> snapshot;
            lock.lock();
            try {
                snapshot = new ArrayList<>(activeSet);
            }finally {
                lock.unlock();
            }
            for (Connection proxy : snapshot) {
                lock.lock();
                boolean isAlive ;
                try {
                    isAlive=activeSet.contains(proxy);
                } finally {
                    lock.unlock();
                }
                //不在借出set中，说明被归还了，跳过
                if(!isAlive){
                    continue;
                }
                PooledConnection handler = (PooledConnection) Proxy.getInvocationHandler(proxy);
                long elapsed = now - handler.getBorrowTime();
                //生产上 HikariCP 也是这么做的，leakDetectionThreshold > 0 时才采集堆栈，平时零开销。
                if (elapsed > config.getLeakDetectionThreshold()) {
                    System.out.println("连接泄漏告警! 借出已 " + elapsed + "ms, proxy=" + proxy + ", 借出栈跟踪=" + handler.getBorrowStackTrace());
                }
                if (elapsed > config.getLeakForceCloseThreshold()) {
                    System.out.println("连接泄漏强制回收! proxy=" + proxy);
                    poolMetrics.getLeakCount().increment();
                    forceCloseConnection(proxy);                    
                }
            }
        }, config.getLeakDetectionInterval(), 
        config.getLeakDetectionInterval(), TimeUnit.SECONDS);
    }
    // 强制关闭连接  必须加锁
    private void forceCloseConnection(Connection proxy) {
        lock.lock();
        try {
            // 二次检查：确保还在 activeSet 中（防止 TOCTOU）
            if(!activeSet.remove(proxy)){
                return;// 已经被正常归还了，不用强制关闭
            }
           Connection real = PooledConnection.unwrap(proxy);
           closeQuietly(real);
           totalCount.decrementAndGet();
           updateRealtimeMetrics();
        } finally{
            lock.unlock();
        }
        
    }
    void startIdleEviction() {
        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("idleEvictionInterval=" + config.getIdleEvictionInterval() + ", maxIdleTime=" + config.getMaxIdleTime());
            
            lock.lock();
            try {
                Iterator<Connection> it = idleQueue.iterator();
                long now = System.currentTimeMillis();
                while(it.hasNext()){
                    Connection conn = it.next();
                    if(totalCount.get()<=config.getMinIdle()){
                        break;
                    }
                    Long returnTime = returnTimeMap.get(conn);
                    System.out.println("连接空闲检查! proxy=" + conn + ", 归还时间=" + returnTime + ", 空闲时间=" + (now - returnTime) + "ms");
                    if (returnTime != null && now - returnTime > config.getMaxIdleTime()) {
                        it.remove();
                        returnTimeMap.remove(conn);
                        closeQuietly(conn);
                        totalCount.decrementAndGet();
                        System.out.println("连接空闲强制回收! proxy=" + conn);
                    }
                }
                updateRealtimeMetrics();
            } finally {
                lock.unlock();
            }
            
        }, config.getIdleEvictionInterval(), 
        config.getIdleEvictionInterval(), TimeUnit.SECONDS);
    }
 
}

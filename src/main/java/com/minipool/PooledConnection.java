package com.minipool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;

public class PooledConnection implements InvocationHandler {

    private final Connection realConnection;
    private final ConnectionPool pool;
    private volatile long borrowTime;
    private volatile Throwable borrowStackTrace;
    

    public PooledConnection(Connection realConnection, ConnectionPool pool) {
        this.realConnection = realConnection;
        this.pool = pool;
        this.borrowTime = System.currentTimeMillis();
    }

    public static Connection wrap(Connection real, ConnectionPool pool) {
        PooledConnection pc = new PooledConnection(real, pool);
        if (pool.getConfig().getLeakDetectionThreshold() > 0) {
            pc.borrowStackTrace = new Throwable("连接借出堆栈");
        }
        return (Connection) Proxy.newProxyInstance(
            real.getClass().getClassLoader(),
            new Class[]{Connection.class},
            pc
        );
    }

    public static Connection unwrap(Connection proxy) {
        InvocationHandler handler = Proxy.getInvocationHandler(proxy);
        PooledConnection pooled = (PooledConnection) handler;
        return pooled.realConnection;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if ("close".equals(method.getName())) {
            pool.releaseConnection((Connection) proxy);
            return null;
        }
        return method.invoke(realConnection, args);
    }

    public long getBorrowTime() { return borrowTime; }
    public void setBorrowTime(long borrowTime) { this.borrowTime = borrowTime; }
    public Throwable getBorrowStackTrace() { return borrowStackTrace; }
    
 
    public Connection getRealConnection() { return realConnection; }
    public ConnectionPool getPool() { return pool; }
}

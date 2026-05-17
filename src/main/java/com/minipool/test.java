package com.minipool;

import com.minipool.ConnectionPool;
import com.minipool.PoolConfig;

import java.sql.Connection;

public class test {
        public static void testBorrowAndReturn() throws Exception {
        PoolConfig config = new PoolConfig("jdbc:h2:mem:testdb", "sa", "sa", 5, 10, 3000L);

        PoolDataSource poolDataSource =new PoolDataSource(config);
        Connection c1= poolDataSource.getConnection();
        System.out.println("1");
        Connection c2= poolDataSource.getConnection();
        System.out.println("2");
        Connection c3= poolDataSource.getConnection();
        System.out.println("3");
        Connection c4= poolDataSource.getConnection();
        System.out.println("4");
        Connection c5= poolDataSource.getConnection();
        System.out.println("5");
 
        Connection c6= poolDataSource.getConnection();
        System.out.println("6");
        c5.close();
        c6.close();
        Thread.sleep(20001);
        System.out.println("totalCount=" + poolDataSource.getConnectionPool().getTotalCount());
        poolDataSource.close();

    }
    public static void main(String[] args) throws Exception {
        testBorrowAndReturn();
    }
}

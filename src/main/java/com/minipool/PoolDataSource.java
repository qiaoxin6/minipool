package com.minipool;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import lombok.Data;


@Data
public class PoolDataSource implements DataSource {
    private ConnectionPool connectionPool;
    private PoolConfig config;

    public PoolDataSource(PoolConfig config) throws SQLException {
        this.config = config;
        this.connectionPool = new ConnectionPool(config);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connectionPool.borrowConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public void close() throws SQLException {
        connectionPool.shutdown();
    }

    public ConnectionPool getConnectionPool() {
        return connectionPool;
    }
}

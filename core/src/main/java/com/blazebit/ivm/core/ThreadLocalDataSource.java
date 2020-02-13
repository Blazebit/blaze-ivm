package com.blazebit.ivm.core;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class ThreadLocalDataSource implements DataSource {

    public static final ThreadLocalDataSource INSTANCE = new ThreadLocalDataSource();
    public static final ThreadLocal<Connection> CONNECTION = new ThreadLocal<>();

    private ThreadLocalDataSource() {
    }

    @Override
    public Connection getConnection() throws SQLException {
        return CONNECTION.get();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return CONNECTION.get();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
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
        return null;
    }
}
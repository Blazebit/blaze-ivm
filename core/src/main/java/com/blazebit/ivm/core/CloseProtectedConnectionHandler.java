package com.blazebit.ivm.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;

public class CloseProtectedConnectionHandler implements InvocationHandler {

    private final Object connection;

    public CloseProtectedConnectionHandler(Object connection) {
        this.connection = connection;
    }

    public static Connection wrap(Connection connection) {
        return (Connection) Proxy.newProxyInstance(
            connection.getClass().getClassLoader(),
            new Class[]{ Connection.class },
            new CloseProtectedConnectionHandler(connection)
        );
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (Statement.class.isAssignableFrom(method.getReturnType())
            || DatabaseMetaData.class.isAssignableFrom(method.getReturnType())) {
            Object delegate = method.invoke(connection, args);
            return Proxy.newProxyInstance(
                connection.getClass().getClassLoader(),
                new Class[]{ method.getReturnType() },
                new ConnectionReturningHandler(proxy, delegate)
            );
        } else if ("close".equals(method.getName())) {
            return null;
        } else {
            return method.invoke(connection, args);
        }
    }
}
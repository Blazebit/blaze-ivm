package com.blazebit.ivm.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;

public class ConnectionReturningHandler implements InvocationHandler {

    private final Object connection;
    private final Object statement;

    public ConnectionReturningHandler(Object connection, Object statement) {
        this.connection = connection;
        this.statement = statement;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (Connection.class.isAssignableFrom(method.getReturnType())) {
            return connection;
        } else if (ResultSet.class.isAssignableFrom(method.getReturnType())) {
            Object delegate = method.invoke(statement, args);
            return Proxy.newProxyInstance(
                connection.getClass().getClassLoader(),
                new Class[]{ method.getReturnType() },
                new StatementReturningHandler(connection, proxy, delegate)
            );
        } else {
            return method.invoke(statement, args);
        }
    }
}
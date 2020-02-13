package com.blazebit.ivm.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Statement;

public class StatementReturningHandler implements InvocationHandler {

    private final Object connection;
    private final Object statement;
    private final Object resultSet;

    public StatementReturningHandler(Object connection, Object statement, Object resultSet) {
        this.connection = connection;
        this.statement = statement;
        this.resultSet = resultSet;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (Connection.class.isAssignableFrom(method.getReturnType())) {
            return connection;
        } else if (Statement.class.isAssignableFrom(method.getReturnType())) {
            return statement;
        } else {
            return method.invoke(resultSet, args);
        }
    }
}
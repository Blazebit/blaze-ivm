/*
 * Copyright 2020 - 2020 Blazebit.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
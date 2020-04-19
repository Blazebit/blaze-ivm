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
package com.blazebit.ivm.testsuite.cleaner;

import java.sql.Connection;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
public interface DatabaseCleaner {

    public static interface Factory {

        public DatabaseCleaner create();

    }

    public void addIgnoredTable(String tableName);

    public boolean isApplicable(Connection connection);

    public boolean supportsClearSchema();

    public void clearSchema(Connection connection);

    public void clearData(Connection connection);

}

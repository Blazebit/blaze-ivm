package com.blazebit.ivm.testsuite;

import com.blazebit.ivm.core.CloseProtectedConnectionHandler;
import com.blazebit.ivm.core.ThreadLocalDataSource;
import com.blazebit.ivm.core.TriggerBasedIvmStrategy;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.hibernate.engine.spi.SessionImplementor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 * @author Moritz Becker
 * @author Christian Beikov
 * @since 1.0.0
 */
public abstract class MaterializationTest extends AbstractHibernatePersistenceTest {

    protected Map<String, TriggerBasedIvmStrategy.TriggerDefinition> setupMaterialization(String viewQuery) {
        try {
            String materializationTableName = "trigger_mat";
            Connection connection = em.unwrap(SessionImplementor.class).connection();
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP MATERIALIZED VIEW IF EXISTS native_mat");
                statement.execute("DROP TABLE IF EXISTS " + materializationTableName + " CASCADE");
                statement.execute("CREATE MATERIALIZED VIEW native_mat AS " + viewQuery);
            }
            connection.commit();
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE ").append(materializationTableName).append(" (");
            try (ResultSet rs = connection.getMetaData().getColumns(null, null, "native_mat", null)) {
                while (rs.next()) {
                    sb.append(rs.getString("COLUMN_NAME"));
                    sb.append(' ').append(rs.getString("TYPE_NAME"));
                    sb.append(',');
                }
            }
            sb.setCharAt(sb.length() - 1, ')');
            try (Statement statement = connection.createStatement()) {
                statement.execute(sb.toString());
            }
            connection.commit();

            ThreadLocalDataSource.CONNECTION.set(CloseProtectedConnectionHandler.wrap(connection));

            CalciteConnection calciteConnection = new Driver().connect("jdbc:calcite:", null).unwrap(CalciteConnection.class);
            String name = "adhoc";
            String schema;
            String catalog;
            if (connection.getMetaData().getJDBCMinorVersion() > 0) {
                schema = connection.getSchema();
            } else {
                schema = null;
            }
            catalog = null;

            JdbcSchema jdbcSchema = JdbcSchema.create(
                calciteConnection.getRootSchema(),
                name,
                ThreadLocalDataSource.INSTANCE,
                catalog,
                schema
            );
            calciteConnection.getRootSchema().add(name, jdbcSchema);
            calciteConnection.setSchema(name);

            TriggerBasedIvmStrategy triggerBasedIvmStrategy = new TriggerBasedIvmStrategy(calciteConnection, viewQuery, materializationTableName);
            Map<String, TriggerBasedIvmStrategy.TriggerDefinition> triggerDefinitions = triggerBasedIvmStrategy.generateTriggerDefinitionForBaseTable();
            for (TriggerBasedIvmStrategy.TriggerDefinition triggerDefinition : triggerDefinitions.values()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute(triggerDefinition.getDropScript());
                }
            }
            connection.commit();
            for (TriggerBasedIvmStrategy.TriggerDefinition triggerDefinition : triggerDefinitions.values()) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute(triggerDefinition.getCreateScript());
                }
            }
            connection.commit();
            try (Statement statement = connection.createStatement()) {
                statement.execute("INSERT INTO " + materializationTableName + " " + viewQuery);
            }
            connection.commit();
            return triggerDefinitions;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            ThreadLocalDataSource.CONNECTION.remove();
        }
    }

    protected void assertMaterializationEqual() {
        try {
            Connection connection = em.unwrap(SessionImplementor.class).connection();
            try (Statement statement = connection.createStatement()) {
                statement.execute("REFRESH MATERIALIZED VIEW native_mat");
            }
            connection.commit();
            List<String> columns = new ArrayList<>();
            try (ResultSet rs = connection.getMetaData().getColumns(null, null, "native_mat", null)) {
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME"));
                }
            }
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            appendSelect(sb, "native_mat", columns);
            sb.append(" EXCEPT ");
            appendSelect(sb, "trigger_mat", columns);
            sb.append(") UNION ALL (");
            appendSelect(sb, "trigger_mat", columns);
            sb.append(" EXCEPT ");
            appendSelect(sb, "native_mat", columns);
            sb.append(')');
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery(sb.toString())) {
                    if (rs.next()) {
                        StringBuilder rows = new StringBuilder();
                        rows.append("Native and trigger materialized tables are not equal!\n");
                        for (String column : columns) {
                            rows.append(column).append('|');
                        }
                        rows.setCharAt(rows.length() - 1, '\n');
                        do {
                            for (int i = 1; i <= columns.size(); i++) {
                                rows.append(rs.getString(i)).append('|');
                            }
                            rows.setCharAt(rows.length() - 1, '\n');
                        } while (rs.next());
                        fail(rows.toString());
                    }
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void appendSelect(StringBuilder sb, String table, List<String> columns) {
        sb.append("SELECT ");
        for (String column : columns) {
            sb.append(column).append(',');
        }

        sb.setCharAt(sb.length() - 1, ' ');
        sb.append("FROM ").append(table);
    }

}

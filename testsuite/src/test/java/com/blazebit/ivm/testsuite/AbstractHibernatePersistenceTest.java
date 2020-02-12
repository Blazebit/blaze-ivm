package com.blazebit.ivm.testsuite;

import com.blazebit.ivm.testsuite.cleaner.DatabaseCleaner;
import org.hibernate.engine.spi.SessionImplementor;

import javax.persistence.EntityManager;
import java.sql.Connection;
import java.util.Properties;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
public abstract class AbstractHibernatePersistenceTest extends AbstractJpaPersistenceTest {

    @Override
    protected Connection getConnection(EntityManager em) {
        return em.unwrap(SessionImplementor.class).connection();
    }

    @Override
    protected void addIgnores(DatabaseCleaner applicableCleaner) {
        applicableCleaner.addIgnoredTable("hibernate_sequence");
    }

    @Override
    protected Properties applyProperties(Properties properties) {
        if (System.getProperty("hibernate.dialect") != null) {
            properties.put("hibernate.dialect", System.getProperty("hibernate.dialect"));
        } /*else if (properties.get("javax.persistence.jdbc.url").toString().contains("mysql")) {
            // MySQL is drunk, it does stuff case insensitive by default...
            properties.put("hibernate.dialect", SaneMySQLDialect.class.getName());

            // Since MySQL has no sequences, the native strategy is needed for batch inserts
            properties.put("hibernate.id.new_generator_mappings", "false");
        }*//* else if (properties.get("javax.persistence.jdbc.url").toString().contains("db2")) {
            // The original DB2 dialect misses support for sequence retrieve in select statements
            properties.put("hibernate.dialect", SaneDB2Dialect.class.getName());
        }*/ else if (properties.get("javax.persistence.jdbc.url").toString().contains("h2")) {
            // Hibernate 5 uses sequences by default but h2 seems to have a bug with sequences in a limited query
            properties.put("hibernate.id.new_generator_mappings", "false");
        }/* else if (properties.get("javax.persistence.jdbc.url").toString().contains("sqlserver")) {
            // Apparently the dialect resolver doesn't choose the latest dialect
            properties.put("hibernate.dialect", SQLServer2012Dialect.class.getName());
            // Not sure what is happening, but when the sequence is tried to be fetched, it doesn't exist in SQL Server
            if (isHibernate5()) {
                properties.put("hibernate.id.new_generator_mappings", "false");
            }
        }*/

        // Needed for Envers tests in Hibernate >= 5.3.5, 5.4.x (HHH-12871)
        properties.put("hibernate.ejb.metamodel.population", "enabled");
        // We use the following only for debugging purposes
        // Normally these settings should be disabled since the output would be too big TravisCI
//        properties.put("hibernate.show_sql", "true");
//        properties.put("hibernate.format_sql", "true");
        return properties;
    }

    @Override
    protected void configurePersistenceUnitInfo(MutablePersistenceUnitInfo persistenceUnitInfo) { }
}

package com.blazebit.ivm.core;

import org.apache.calcite.sql.SqlNode;

import java.util.Set;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
public class Term {
    private final Set<String> tables;
    private String predicate;

    public Term(Set<String> tables, String predicate) {
        this.tables = tables;
        this.predicate = predicate;
    }

    public Set<String> getTables() {
        return tables;
    }

    public String getPredicate() {
        return predicate;
    }

    public void addPredicateConjunct(SqlNode conjunct) {
        predicate  += " AND " + conjunct;
    }
}

package com.blazebit.ivm.core;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
public class Term {
    private final Set<RelOptTable> tables;
    private RexNode predicate;

    public Term(Set<RelOptTable> tables, RexNode predicate) {
        this.tables = tables;
        this.predicate = predicate;
    }

    public Term(RexBuilder rexBuilder, Term t1, Term t2, RexNode predicate) {
        this.tables = new HashSet<>(t1.tables.size() + t2.tables.size());
        this.tables.addAll(t1.tables);
        this.tables.addAll(t2.tables);
        this.predicate = RexUtil.composeConjunction(rexBuilder, Arrays.asList(t1.predicate, t2.predicate, predicate));
    }

    public Set<RelOptTable> getTables() {
        return tables;
    }

    public RexNode getPredicate() {
        return predicate;
    }

    public void addPredicate(RexBuilder rexBuilder, RexNode predicate) {
        this.predicate = RexUtil.composeConjunction(rexBuilder, Arrays.asList(this.predicate, predicate));
    }

}

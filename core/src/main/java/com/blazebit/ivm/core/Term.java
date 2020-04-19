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

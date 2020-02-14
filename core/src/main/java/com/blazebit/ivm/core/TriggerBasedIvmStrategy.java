package com.blazebit.ivm.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
public class TriggerBasedIvmStrategy {

    private final SqlDialect dialect;
    private final RexBuilder rexBuilder;
    private final RelRoot relRoot;
    private final String materializationTableName;
    private final Table materializationTable;

    public TriggerBasedIvmStrategy(CalciteConnection calciteConnection, String viewSqlQuery, String materializationTableName) {
        try {
            final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(
                    SqlParser.configBuilder()
                        .setLex(Lex.MYSQL)
                        .setConformance(SqlConformanceEnum.BABEL)
                        .setQuoting(Quoting.BACK_TICK)
                        .setUnquotedCasing(Casing.UNCHANGED)
                        .build()
                )
                .defaultSchema(calciteConnection.getRootSchema().getSubSchema("adhoc"))
                .build();
            PlannerImpl planner = new PlannerImpl(config);
            SqlNode sqlNode = planner.parse(viewSqlQuery);
            planner.validate(sqlNode);
            this.dialect = config.getDefaultSchema().unwrap(JdbcSchema.class).dialect;
            this.relRoot = planner.rel(sqlNode);
            this.rexBuilder = new RexBuilder(planner.getTypeFactory());
            this.materializationTableName = materializationTableName;
            this.materializationTable = config.getDefaultSchema().getTable(materializationTableName);
        } catch (Exception e) {
            throw new RuntimeException("parse failed", e);
        }
    }

    public Map<String, TriggerDefinition> generateTriggerDefinitionForBaseTable() {
        // produce join-disjunctive normal form
        Set<Term> normalize = normalize(relRoot.rel);
        // create subsumption graph

        // create insert trigger definition
        // - create query for direct delta
        // - apply direct
        // - create query for indirect delta
        // - apply indirect delta

        // create delete trigger definition
        // - create query for direct delta
        // - apply direct
        // - create query for indirect delta
        // - apply indirect delta

        Map<String, TriggerDefinition> triggers = new HashMap<>();
        final RelMetadataQuery metadataQuery = relRoot.rel.getCluster().getMetadataQuery();
        for (RexTableInputRef.RelTableRef tableReference : metadataQuery.getTableReferences(relRoot.rel)) {
            StringBuilder triggerSb = new StringBuilder();
            String tableName = tableReference.getQualifiedName().get(1);
            String triggerName = tableName + "_trig";
            String triggerFunctionName = tableName + "_trig_fn";
            String dropScript = "DROP TRIGGER IF EXISTS " + triggerName + " ON " + tableName + "; DROP FUNCTION IF EXISTS " + triggerFunctionName + ";";
            triggerSb.append("CREATE FUNCTION ").append(triggerFunctionName).append(" RETURNS trigger AS \n$BODY$\nBEGIN\n");
            generateTrigger(triggerSb, normalize, tableReference.getTable());
            triggerSb.append("\nRETURN NEW;");
            triggerSb.append("\nEND;\n$BODY$;\n");
            triggerSb.append("CREATE TRIGGER ").append(triggerName).append(" AFTER INSERT OR UPDATE OR DELETE ON ").append(tableName).append(" FOR EACH ROW EXECUTE PROCEDURE ").append(triggerFunctionName).append("();");
            triggers.put(tableName, new TriggerDefinition(dropScript, triggerSb.toString()));
        }

        return triggers;
    }

    private static JoinRelType forExcludeRightNullExtended(JoinRelType joinType) {
        if (joinType == JoinRelType.FULL) {
            return JoinRelType.LEFT;
        } else if (joinType == JoinRelType.RIGHT) {
            return JoinRelType.INNER;
        }
        return joinType;
    }

    private void generateTrigger(StringBuilder sb, Set<Term> normalizedTerms, RelOptTable table) {
        // The most important step here is to move the table T to the left
        // In addition, we rewrite joins to exclude null-extended tuples
//        LogicalTableScan logicalTableScan = LogicalTableScan.create(relRoot.rel.getCluster(), RelOptTableImpl.create(table.getRelOptSchema(), table.getRowType(), table.unwrap(Table.class), ImmutableList.of("NEW")));
//        SqlOperator sqlOperator = rexBuilder.getOpTab().getOperatorList().stream().filter(o -> "=".equals(o.getName())).findFirst().get();
        RelNode deltaVDRelNode = relRoot.rel.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalJoin join) {
                JoinRelType joinType = join.getJoinType();
                // For vD we only care about non-null extending tuples, so we transform joins to exclude null extended tuples
                if (table.equals(join.getLeft().getTable())) {
                    joinType = forExcludeRightNullExtended(joinType);
//                    RexUtil.composeConjunction(rexBuilder, Arrays.asList(join.getCondition(), rexBuilder.makeCall(sqlOperator, logicalTableScan.)))
                    RexNode condition = join.getCondition();
                    return join.copy(join.getTraitSet(), condition, join.getLeft(), join.getRight(), joinType, join.isSemiJoinDone());
                } else if (table.equals(join.getRight().getTable())) {
                    joinType = forExcludeRightNullExtended(joinType.swap());
                    return join.copy(join.getTraitSet(), join.getCondition(), join.getRight(), join.getLeft(), joinType, join.isSemiJoinDone());
                } else {
                    RelNode left = join.getLeft().accept(this);
                    RelNode right = join.getRight().accept(this);
                    if (left == join.getLeft()) {
                        // Nothing changed on the left side
                        if (right == join.getRight()) {
                            // Nothing changed, no source table in this join
                            return join;
                        }

                        // Source table on the right side, so we have to swap
                        JoinRelType newJoinType = forExcludeRightNullExtended(joinType.swap());
                        return join.copy(join.getTraitSet(), join.getCondition(), right, left, newJoinType, join.isSemiJoinDone());
                    } else {
                        // The left side contains the source table
                        JoinRelType newJoinType = forExcludeRightNullExtended(joinType);
                        return join.copy(join.getTraitSet(), join.getCondition(), left, right, newJoinType, join.isSemiJoinDone());
                    }
                }
            }
        });


        sb.append("CREATE TEMPORARY TABLE _delta1 AS ");

        SqlImplementor.Result result = new RelToSqlConverter(dialect).visitChild(0, deltaVDRelNode);
        sb.append(new SqlPrettyWriter(dialect).format(result.asSelect())).append(" AND NEW.order_id = 1");
        sb.append("IF OLD IS NOT NULL THEN\n");
        // DELETE
        sb.append("DELETE FROM ").append(materializationTableName).append(" a WHERE ");

        sb.append(";\nEND IF;\n");


        sb.append("IF NEW IS NOT NULL THEN\n");
        // INSERT
        sb.append("INSERT INTO ").append(materializationTableName).append("(");
        List<String> fieldNames = materializationTable.getRowType(rexBuilder.getTypeFactory()).getFieldNames();
        for (String fieldName : fieldNames) {
            sb.append(fieldName).append(',');
        }
        sb.setCharAt(sb.length() - 1, ')');
        sb.append(" SELECT ");

        sb.append(" FROM (VALUES(1)) a");
        sb.append(";\nEND IF;\n");
    }

    public static class TriggerDefinition {
        private final String dropScript;
        private final String createScript;

        public TriggerDefinition(String dropScript, String createScript) {
            this.dropScript = dropScript;
            this.createScript = createScript;
        }

        public String getDropScript() {
            return dropScript;
        }

        public String getCreateScript() {
            return createScript;
        }
    }

    private Set<Term> normalize(RelNode relNode) {
        if (relNode instanceof LogicalProject) {
            Set<Term> terms = new HashSet<>();
            RelNode selectInput = relNode.getInput(0);
            RexNode predicate;
            Set<RelOptTable> nullRejectedTables;
            if (selectInput instanceof LogicalFilter) {
                predicate = ((LogicalFilter) selectInput).getCondition();
                nullRejectedTables = getNullRejectedTables(selectInput, predicate);
                selectInput = selectInput.getInput(0);
            } else {
                predicate = RexUtil.composeConjunction(rexBuilder, Collections.emptyList());
                nullRejectedTables = Collections.emptySet();
            }
            terms.addAll(normalize(selectInput));
            for (Iterator<Term> iterator = terms.iterator(); iterator.hasNext(); ) {
                Term term = iterator.next();
                if (term.getTables().containsAll(nullRejectedTables)) {
                    iterator.remove();
                } else {
                    term.addPredicate(rexBuilder, predicate);
                }
            }
            return terms;
        } else if (relNode instanceof LogicalJoin) {
            LogicalJoin join = (LogicalJoin) relNode;
            Set<Term> leftTerms = normalize(join.getLeft());
            Set<Term> rightTerms = normalize(join.getRight());
            Set<Term> newTerms = new HashSet<>();
            Set<Term> eliminatedTerms = new HashSet<>();
            Set<RelOptTable> nullRejectedTables = getNullRejectedTables(relNode, join.getCondition());
            for (Term leftTerm : leftTerms) {
                for (Term rightTerm : rightTerms) {
                    Term t = new Term(rexBuilder, leftTerm, rightTerm, join.getCondition());
                    if (!t.getTables().containsAll(nullRejectedTables)) {
                        newTerms.add(t);
                        if (isSubsumed(leftTerm, t)) {
                            eliminatedTerms.add(leftTerm);
                        }
                        if (isSubsumed(rightTerm, t)) {
                            eliminatedTerms.add(rightTerm);
                        }
                    }
                }
            }
            switch (join.getJoinType()) {
                case FULL:
                    newTerms.addAll(leftTerms);
                    newTerms.addAll(rightTerms);
                    break;
                case LEFT:
                    newTerms.addAll(leftTerms);
                    break;
                case RIGHT:
                    newTerms.addAll(rightTerms);
                    break;
                case INNER:
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported join type: " + join.getJoinType());
            }
            newTerms.removeAll(eliminatedTerms);
            return newTerms;
        } else if (relNode instanceof TableScan) {
            return Collections.singleton(new Term(Collections.singleton(relNode.getTable()), RexUtil.composeConjunction(rexBuilder, Collections.emptyList())));
        } else {
            throw new IllegalArgumentException("Unsupported rel node: " + relNode);
        }
    }

    private Set<RelOptTable> getNullRejectedTables(RelNode relNode, RexNode predicate) {
        final Set<RelOptTable> tables = new HashSet<>();
        final RelMetadataQuery metadataQuery = relRoot.rel.getCluster().getMetadataQuery();
        predicate.accept(new RexVisitorImpl<Boolean>(false) {
            @Override
            public Boolean visitCall(RexCall call) {
                if (call.getType().isNullable()) {
                    return Boolean.TRUE;
                }
                for (RexNode operand : call.getOperands()) {
                    if (operand.accept(this) == Boolean.TRUE) {
                        break;
                    }
                }

                return null;
            }

            @Override
            public Boolean visitTableInputRef(RexTableInputRef ref) {
                tables.add(ref.getTableRef().getTable());
                return null;
            }

            @Override
            public Boolean visitInputRef(RexInputRef inputRef) {
                Set<RexNode> expressionLineage = metadataQuery.getExpressionLineage(relNode, inputRef);
                if (expressionLineage != null) {
                    for (RexNode rexNode : expressionLineage) {
                        rexNode.accept(this);
                    }
                }

                return null;
            }
        });

        return tables;
    }

    private boolean isSubsumed(Term subTerm, Term superTerm) {
        // See https://github.com/apache/calcite/blob/master/core/src/main/java/org/apache/calcite/rel/rules/AbstractMaterializedViewRule.java

//        1. Compute equivalence classes for the query and the view.
//        2. Check that every view equivalence class is a subset of a
//            query equivalence class. If not, reject the view
//        3. Compute range intervals for the query and the view.
//        4. Check that every view range contains the corresponding
//        query range. If not, reject the view.
//        5. Check that every conjunct in the residual predicate of the
//        view matches a conjunct in the residual predicate of the
//        query. If not, reject the view.
        if (!superTerm.getTables().containsAll(subTerm.getTables())) {
            return false;
        }
        final EquivalenceClasses subEC = new EquivalenceClasses();
        Pair<RexNode, RexNode> subPredPair = splitPredicates(rexBuilder, subTerm.getPredicate());
        for (RexNode conj : RelOptUtil.conjunctions(subPredPair.left)) {
            RexCall equiCond = (RexCall) conj;
            subEC.addEquivalenceClass(
                (RexTableInputRef) equiCond.getOperands().get(0),
                (RexTableInputRef) equiCond.getOperands().get(1));
        }

        final EquivalenceClasses superEC = new EquivalenceClasses();
        Pair<RexNode, RexNode> superPredPair = splitPredicates(rexBuilder, superTerm.getPredicate());
        for (RexNode conj : RelOptUtil.conjunctions(superPredPair.left)) {
            RexCall equiCond = (RexCall) conj;
            superEC.addEquivalenceClass(
                (RexTableInputRef) equiCond.getOperands().get(0),
                (RexTableInputRef) equiCond.getOperands().get(1));
        }
        for (Map.Entry<RexTableInputRef, Set<RexTableInputRef>> entry : subEC.getEquivalenceClassesMap().entrySet()) {
            Set<RexTableInputRef> rexTableInputRefs = superEC.getEquivalenceClassesMap().get(entry.getKey());
            if (rexTableInputRefs == null || !rexTableInputRefs.containsAll(entry.getValue())) {
                return false;
            }
        }

        return true;
    }
    /**
     * Classifies each of the predicates in the list into one of these two
     * categories:
     *
     * <ul>
     * <li> 1-l) column equality predicates, or
     * <li> 2-r) residual predicates, all the rest
     * </ul>
     *
     * <p>For each category, it creates the conjunction of the predicates. The
     * result is an pair of RexNode objects corresponding to each category.
     */
    private static Pair<RexNode, RexNode> splitPredicates(
        RexBuilder rexBuilder, RexNode pred) {
        List<RexNode> equiColumnsPreds = new ArrayList<>();
        List<RexNode> residualPreds = new ArrayList<>();
        for (RexNode e : RelOptUtil.conjunctions(pred)) {
            switch (e.getKind()) {
                case EQUALS:
                    RexCall eqCall = (RexCall) e;
                    if (RexUtil.isReferenceOrAccess(eqCall.getOperands().get(0), false)
                        && RexUtil.isReferenceOrAccess(eqCall.getOperands().get(1), false)) {
                        equiColumnsPreds.add(e);
                    } else {
                        residualPreds.add(e);
                    }
                    break;
                default:
                    residualPreds.add(e);
            }
        }
        return Pair.of(
            RexUtil.composeConjunction(rexBuilder, equiColumnsPreds),
            RexUtil.composeConjunction(rexBuilder, residualPreds));
    }

    private static class EquivalenceClasses {

        private final Map<RexTableInputRef, Set<RexTableInputRef>> nodeToEquivalenceClass;
        private Map<RexTableInputRef, Set<RexTableInputRef>> cacheEquivalenceClassesMap;
        private List<Set<RexTableInputRef>> cacheEquivalenceClasses;

        protected EquivalenceClasses() {
            nodeToEquivalenceClass = new HashMap<>();
            cacheEquivalenceClassesMap = ImmutableMap.of();
            cacheEquivalenceClasses = ImmutableList.of();
        }

        protected void addEquivalenceClass(RexTableInputRef p1, RexTableInputRef p2) {
            // Clear cache
            cacheEquivalenceClassesMap = null;
            cacheEquivalenceClasses = null;

            Set<RexTableInputRef> c1 = nodeToEquivalenceClass.get(p1);
            Set<RexTableInputRef> c2 = nodeToEquivalenceClass.get(p2);
            if (c1 != null && c2 != null) {
                // Both present, we need to merge
                if (c1.size() < c2.size()) {
                    // We swap them to merge
                    Set<RexTableInputRef> c2Temp = c2;
                    c2 = c1;
                    c1 = c2Temp;
                }
                for (RexTableInputRef newRef : c2) {
                    c1.add(newRef);
                    nodeToEquivalenceClass.put(newRef, c1);
                }
            } else if (c1 != null) {
                // p1 present, we need to merge into it
                c1.add(p2);
                nodeToEquivalenceClass.put(p2, c1);
            } else if (c2 != null) {
                // p2 present, we need to merge into it
                c2.add(p1);
                nodeToEquivalenceClass.put(p1, c2);
            } else {
                // None are present, add to same equivalence class
                Set<RexTableInputRef> equivalenceClass = new LinkedHashSet<>();
                equivalenceClass.add(p1);
                equivalenceClass.add(p2);
                nodeToEquivalenceClass.put(p1, equivalenceClass);
                nodeToEquivalenceClass.put(p2, equivalenceClass);
            }
        }

        protected Map<RexTableInputRef, Set<RexTableInputRef>> getEquivalenceClassesMap() {
            if (cacheEquivalenceClassesMap == null) {
                cacheEquivalenceClassesMap = ImmutableMap.copyOf(nodeToEquivalenceClass);
            }
            return cacheEquivalenceClassesMap;
        }

        protected List<Set<RexTableInputRef>> getEquivalenceClasses() {
            if (cacheEquivalenceClasses == null) {
                Set<RexTableInputRef> visited = new HashSet<>();
                ImmutableList.Builder<Set<RexTableInputRef>> builder =
                    ImmutableList.builder();
                for (Set<RexTableInputRef> set : nodeToEquivalenceClass.values()) {
                    if (Collections.disjoint(visited, set)) {
                        builder.add(set);
                        visited.addAll(set);
                    }
                }
                cacheEquivalenceClasses = builder.build();
            }
            return cacheEquivalenceClasses;
        }

        protected static EquivalenceClasses copy(EquivalenceClasses ec) {
            final EquivalenceClasses newEc = new EquivalenceClasses();
            for (Map.Entry<RexTableInputRef, Set<RexTableInputRef>> e
                : ec.nodeToEquivalenceClass.entrySet()) {
                newEc.nodeToEquivalenceClass.put(
                    e.getKey(), Sets.newLinkedHashSet(e.getValue()));
            }
            newEc.cacheEquivalenceClassesMap = null;
            newEc.cacheEquivalenceClasses = null;
            return newEc;
        }
    }
}

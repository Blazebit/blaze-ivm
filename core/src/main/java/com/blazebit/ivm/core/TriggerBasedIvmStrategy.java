package com.blazebit.ivm.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
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

    private final FrameworkConfig config;
    private final SqlDialect dialect;
    private final RexBuilder rexBuilder;
    private final SqlNode sqlNode;
    private final RelRoot relRoot;
    private final String materializationTableName;
    private final Table materializationTable;

    public TriggerBasedIvmStrategy(CalciteConnection calciteConnection, String viewSqlQuery, String materializationTableName) {
        try {
            this.config = Frameworks.newConfigBuilder()
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
            this.sqlNode = SqlParser.create(viewSqlQuery, config.getParserConfig()).parseStmt();
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
        for (SqlIdentifier tableNameIdentifier : getTables()) {
            StringBuilder triggerSb = new StringBuilder();
            String tableName = tableNameIdentifier.toString();
            String triggerName = tableName + "_trig";
            String triggerFunctionName = tableName + "_trig_fn";
            String dropScript = "DROP TRIGGER IF EXISTS " + triggerName + " ON " + tableName + "; DROP FUNCTION IF EXISTS " + triggerFunctionName + "();";
            triggerSb.append("CREATE FUNCTION ").append(triggerFunctionName).append("() RETURNS trigger AS \n$$\nBEGIN\n");
            generateTrigger(triggerSb, tableNameIdentifier);
            triggerSb.append("\nRETURN NEW;");
            triggerSb.append("\nEND;\n$$ LANGUAGE 'plpgsql';\n");
            triggerSb.append("CREATE TRIGGER ").append(triggerName).append(" AFTER INSERT OR UPDATE OR DELETE ON ").append(tableName).append(" FOR EACH ROW EXECUTE PROCEDURE ").append(triggerFunctionName).append("();");
            triggers.put(tableName, new TriggerDefinition(dropScript, triggerSb.toString()));
        }
//        final RelMetadataQuery metadataQuery = relRoot.rel.getCluster().getMetadataQuery();
//        for (RexTableInputRef.RelTableRef tableReference : metadataQuery.getTableReferences(relRoot.rel)) {
//            StringBuilder triggerSb = new StringBuilder();
//            String tableName = tableReference.getQualifiedName().get(1);
//            String triggerName = tableName + "_trig";
//            String triggerFunctionName = tableName + "_trig_fn";
//            String dropScript = "DROP TRIGGER IF EXISTS " + triggerName + " ON " + tableName + "; DROP FUNCTION IF EXISTS " + triggerFunctionName + ";";
//            triggerSb.append("CREATE FUNCTION ").append(triggerFunctionName).append("() RETURNS trigger AS \n$$\nBEGIN\n");
//            generateTrigger(triggerSb, normalize, tableReference.getTable());
//            triggerSb.append("\nRETURN NEW;");
//            triggerSb.append("\nEND;\n$$ LANGUAGE 'plpgsql';\n");
//            triggerSb.append("CREATE TRIGGER ").append(triggerName).append(" AFTER INSERT OR UPDATE OR DELETE ON ").append(tableName).append(" FOR EACH ROW EXECUTE PROCEDURE ").append(triggerFunctionName).append("();");
//            triggers.put(tableName, new TriggerDefinition(dropScript, triggerSb.toString()));
//        }

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

    private static SqlLiteral forExcludeRightNullExtended(SqlLiteral joinType) {
        if (joinType.getValue() == JoinType.FULL) {
            return SqlLiteral.createSymbol(JoinType.LEFT, joinType.getParserPosition());
        } else if (joinType.getValue() == JoinType.RIGHT) {
            return SqlLiteral.createSymbol(JoinType.INNER, joinType.getParserPosition());
        }
        return joinType;
    }

    private static SqlLiteral swap(SqlLiteral joinType) {
        if (joinType.getValue() == JoinType.LEFT) {
            return SqlLiteral.createSymbol(JoinType.RIGHT, joinType.getParserPosition());
        } else if (joinType.getValue() == JoinType.LEFT) {
            return SqlLiteral.createSymbol(JoinType.RIGHT, joinType.getParserPosition());
        }
        return joinType;
    }

    private Set<SqlIdentifier> getTables() {
        Set<SqlIdentifier> tables = new HashSet<>();
        sqlNode.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(SqlCall call) {
                if (call instanceof SqlSelect) {
                    SqlNode from = ((SqlSelect) call).getFrom();
                    if (addSqlIdentifier(from)) {
                        return null;
                    } else {
                        return super.visit(call);
                    }
                } else if (call instanceof SqlJoin) {
                    SqlJoin join = (SqlJoin) call;
                    if (!addSqlIdentifier(join.getLeft())) {
                        join.getLeft().accept(this);
                    }
                    if (!addSqlIdentifier(join.getRight())) {
                        join.getRight().accept(this);
                    }
                    return join.getCondition().accept(this);
                } else {
                    return super.visit(call);
                }
            }

            private boolean addSqlIdentifier(SqlNode from) {
                if (from instanceof SqlIdentifier) {
                    tables.add((SqlIdentifier) from);
                    return true;
                } else if (from instanceof SqlBasicCall && ((SqlBasicCall) from).getOperator().getKind() == SqlKind.AS) {
                    tables.add((SqlIdentifier) ((SqlBasicCall) from).getOperands()[0]);
                    return true;
                }
                return false;
            }
        });
        return tables;
    }

    private void generateTrigger(StringBuilder sb, SqlIdentifier baseTable) {
        // Traverse path from the baseTable join to the root and change joins so that baseTable ends up on the very left side
        // Return new baseTable root join node (left side of this node is the baseTable)
        SqlNode newSqlNode = sqlNode.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlCall call) {
                if (call instanceof SqlJoin) {
                    SqlJoin join = (SqlJoin) call;
                    if (baseTable.equals(getFirstIdentifier(join.getLeft()))) {
                        SqlLiteral joinType = forExcludeRightNullExtended(join.getJoinTypeNode());
                        return new SqlJoin(join.getParserPosition(), join.getLeft(), join.isNaturalNode(), joinType, join.getRight(), join.getConditionTypeNode(), join.getCondition());
                    } else if (baseTable.equals(getFirstIdentifier(join.getRight()))) {
                        SqlLiteral joinType = forExcludeRightNullExtended(swap(join.getJoinTypeNode()));
                        return new SqlJoin(join.getParserPosition(), join.getRight(), join.isNaturalNode(), joinType, join.getLeft(), join.getConditionTypeNode(), join.getCondition());
                    } else {
                        SqlNode left = join.getLeft().accept(this);
                        SqlNode right = join.getRight().accept(this);
                        if (left == join.getLeft()) {
                            // Nothing changed on the left side
                            if (right == join.getRight()) {
                                // Nothing changed, baseTable not referenced in this join
                                return join;
                            }

                            // baseTable on the right side, so we have to swap
                            SqlLiteral joinType = forExcludeRightNullExtended(swap(join.getJoinTypeNode()));
                            return new SqlJoin(join.getParserPosition(), right, join.isNaturalNode(), joinType, left, join.getConditionTypeNode(), join.getCondition());
                        } else {
                            // baseTable on the left side
                            SqlLiteral newJoinType = forExcludeRightNullExtended(join.getJoinTypeNode());
                            return new SqlJoin(join.getParserPosition(), left, join.isNaturalNode(), newJoinType, right, join.getConditionTypeNode(), join.getCondition());
                        }
                    }
                } else {
                    return super.visit(call);
                }
            }

            private SqlNode createNewCorrelatedJoinCondition(SqlNode condition, SqlNode node) {
                // TODO: the ctid condition needs to go into the where clause.
                SqlIdentifier alias;
                if (node instanceof SqlIdentifier) {
                    alias = (SqlIdentifier) node;
                } else if (node instanceof SqlBasicCall && ((SqlBasicCall) node).getOperator().getKind() == SqlKind.AS) {
                    alias = (SqlIdentifier) ((SqlBasicCall) node).getOperands()[1];
                } else {
                    throw new IllegalArgumentException("Can't extract alias from node: " + node);
                }
                List<String> keyColumns = Arrays.asList("ctid");
                for (String keyColumn : keyColumns) {
                    condition = new SqlBasicCall(SqlStdOperatorTable.AND, new SqlNode[]{
                        condition,
                        new SqlBasicCall(SqlStdOperatorTable.EQUALS, new SqlNode[]{
                            alias.plus(keyColumn, SqlParserPos.ZERO),

                            new SqlBasicCall(SqlStdOperatorTable.DOT, new SqlNode[]{
                                SqlLiteral.createSymbol(TransitionTable.NEW, SqlParserPos.ZERO),
                                new SqlIdentifier(ImmutableList.of(keyColumn), SqlParserPos.ZERO)
                            }, SqlParserPos.ZERO)
                        }, SqlParserPos.ZERO)
                    }, SqlParserPos.ZERO);
                }

                return condition;
            }

            private SqlIdentifier getFirstIdentifier(SqlNode node) {
                if (node instanceof SqlIdentifier) {
                    return (SqlIdentifier) node;
                } else if (node instanceof SqlBasicCall && ((SqlBasicCall) node).getOperator().getKind() == SqlKind.AS) {
                    return (SqlIdentifier) ((SqlBasicCall) node).getOperands()[0];
                }
                return null;
            }
        });

        SqlNode from = ((SqlSelect) newSqlNode).getFrom();
        SqlIdentifier rootAlias;
        while (from instanceof SqlJoin) {
            from = ((SqlJoin) from).getLeft();
        }
        if (from instanceof SqlIdentifier) {
            rootAlias = (SqlIdentifier) from;
        } else if (from instanceof SqlBasicCall && ((SqlBasicCall) from).getOperator().getKind() == SqlKind.AS) {
            rootAlias = (SqlIdentifier) ((SqlBasicCall) from).getOperands()[1];
        } else {
            throw new IllegalArgumentException("Can't extract alias from node: " + from);
        }
        List<String> keyColumns = Arrays.asList("ctid");
        for (String keyColumn : keyColumns) {
            ((SqlSelect) newSqlNode).setWhere(
                    new SqlBasicCall(SqlStdOperatorTable.AND, new SqlNode[]{
                        ((SqlSelect) newSqlNode).getWhere() == null ? SqlLiteral.createBoolean(true, from.getParserPosition()) : ((SqlSelect) newSqlNode).getWhere(),
                        new SqlBasicCall(SqlStdOperatorTable.EQUALS, new SqlNode[]{
                            rootAlias.plus(keyColumn, SqlParserPos.ZERO),

                            new SqlBasicCall(SqlStdOperatorTable.DOT, new SqlNode[]{
                                SqlLiteral.createSymbol(TransitionTable.NEW, SqlParserPos.ZERO),
                                new SqlIdentifier(ImmutableList.of(keyColumn), SqlParserPos.ZERO)
                            }, SqlParserPos.ZERO)
                        }, SqlParserPos.ZERO)
                    }, SqlParserPos.ZERO)
            );
        }

        sb.append("CREATE TEMPORARY TABLE _delta1 AS ");

        sb.append(new SqlPrettyWriter(dialect).format(newSqlNode));

        sb.append(";\nIF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN\n");
        // DELETE
        sb.append("DELETE FROM ").append(materializationTableName).append(" a WHERE 1=0");

        sb.append(";\nEND IF;\n");


        sb.append("IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN\n");
        // INSERT
        sb.append("INSERT INTO ").append(materializationTableName).append("(");
        List<String> fieldNames = materializationTable.getRowType(rexBuilder.getTypeFactory()).getFieldNames();
        for (String fieldName : fieldNames) {
            sb.append(fieldName).append(',');
        }
        sb.setCharAt(sb.length() - 1, ')');
        sb.append(" SELECT ");
        sb.append("*");
        sb.append(" FROM _delta1 a");
        sb.append(";\nEND IF;\n");
    }

    private static enum TransitionTable {
        OLD,
        NEW;

        @Override
        public String toString() {
            return name() + ".";
        }
    }

    private void generateTrigger(StringBuilder sb, Set<Term> normalizedTerms, RelOptTable table) {
        // The most important step here is to move the table T to the left
        // In addition, we rewrite joins to exclude null-extended tuples
//        LogicalTableScan logicalTableScan = LogicalTableScan.create(relRoot.rel.getCluster(), RelOptTableImpl.create(table.getRelOptSchema(), table.getRowType(), table.unwrap(Table.class), ImmutableList.of("NEW")));
//        SqlOperator sqlOperator = rexBuilder.getOpTab().getOperatorList().stream().filter(o -> "=".equals(o.getName())).findFirst().get();
        RelNode deltaVDRelNode = relRoot.rel.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalJoin join) {
                // For vD we only care about non-null extending tuples, so we transform joins to exclude null extended tuples
                if (table.equals(join.getLeft().getTable())) {
                    JoinRelType joinType = forExcludeRightNullExtended(join.getJoinType());
                    return join.copy(join.getTraitSet(), createNewCorrelatedJoinCondition(join, join.getLeft()), join.getLeft(), join.getRight(), joinType, join.isSemiJoinDone());
                } else if (table.equals(join.getRight().getTable())) {
                    return swap(join.copy(join.getTraitSet(), createNewCorrelatedJoinCondition(join, join.getRight()), join.getLeft(), join.getRight(), join.getJoinType(), join.isSemiJoinDone()));
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
                        return swap(join.copy(join.getTraitSet(), join.getCondition(), left, right, join.getJoinType(), join.isSemiJoinDone()));
                    } else {
                        // The left side contains the source table
                        JoinRelType newJoinType = forExcludeRightNullExtended(join.getJoinType());
                        return join.copy(join.getTraitSet(), join.getCondition(), left, right, newJoinType, join.isSemiJoinDone());
                    }
                }
            }

            private RexNode createNewCorrelatedJoinCondition(LogicalJoin join, RelNode node) {
                Set<ImmutableBitSet> uniqueKeys = node.getCluster().getMetadataQuery().getUniqueKeys(node);
                ImmutableBitSet uniqueFieldBitSet;
                if (uniqueKeys == null) {
                    // TODO: currently calcite doesn't seem to support access to uniqueness metadata
                    uniqueFieldBitSet = ImmutableBitSet.of(0);
                } else {
                    uniqueFieldBitSet = uniqueKeys.iterator().next();
                }
                List<RelDataTypeField> fieldList = node.getRowType().getFieldList();
                RelBuilder relBuilder = RelBuilder.create(config);
                RexNode condition = join.getCondition();
                List<RexNode> conjuncts = new ArrayList<>(uniqueFieldBitSet.size() + 1);
                conjuncts.add(condition);

                for (Integer index : uniqueFieldBitSet) {
                    relBuilder.push(join);
                    conjuncts.add(
                        relBuilder.equals(
                            relBuilder.field(fieldList.get(index).getName()),
                            relBuilder.transientScan("NEW", join.getRowType()).field(fieldList.get(index).getName())
                        )
                    );
                }
                return RexUtil.composeConjunction(rexBuilder, conjuncts);
            }

            private RelNode swap(LogicalJoin join) {
                RelNode relNode = JoinCommuteRule.swap(join, true, RelBuilder.create(config));
                if (relNode instanceof LogicalProject) {
                    join = (LogicalJoin) relNode.getInput(0);
                } else {
                    join = (LogicalJoin) relNode;
                }
                JoinRelType joinType = forExcludeRightNullExtended(join.getJoinType());
                if (joinType != join.getJoinType()) {
                    join = join.copy(join.getTraitSet(), join.getCondition(), join.getLeft(), join.getRight(), joinType, join.isSemiJoinDone());
                }
                if (relNode instanceof LogicalProject) {
                    LogicalProject logicalProject = (LogicalProject) relNode;
                    List<RexNode> projects = new ArrayList<>(logicalProject.getProjects().size());
                    List<RelDataTypeField> projectFieldList = logicalProject.getRowType().getFieldList();
                    List<RelDataTypeField> fieldList = join.getRowType().getFieldList();
                    List<RelDataTypeField> projectTypeList = new ArrayList<>();
                    List<RexNode> logicalProjectProjects = logicalProject.getProjects();
                    for (int i = 0; i < logicalProjectProjects.size(); i++) {
                        RexNode project = logicalProjectProjects.get(i);
                        RelDataTypeField projectDataTypeField = projectFieldList.get(i);
                        RexNode newProject = project.accept(new RexShuttle() {
                            @Override
                            public RexNode visitInputRef(RexInputRef inputRef) {
                                return new RexInputRef(inputRef.getIndex(), fieldList.get(inputRef.getIndex()).getType());
                            }
                        });

                        projectTypeList.add(new RelDataTypeFieldImpl(projectDataTypeField.getName(), projectDataTypeField.getIndex(), newProject.getType()));
                        projects.add(newProject);
                    }


                    relNode = logicalProject.copy(logicalProject.getTraitSet(), join, projects, new RelRecordType(projectTypeList));
                } else {
                    relNode = join;
                }
                return relNode;
            }
        });

        sb.append("CREATE TEMPORARY TABLE _delta1 AS ");

        SqlImplementor.Result result = new RelToSqlConverter(dialect).visitChild(0, deltaVDRelNode);
        sb.append(new SqlPrettyWriter(dialect).format(result.asSelect()));
//        new SqlPrettyWriter(dialect).format(table.unwrap(JdbcTable.class).tableName().getComponent(1));
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

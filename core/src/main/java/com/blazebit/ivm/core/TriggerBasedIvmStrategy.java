package com.blazebit.ivm.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.type.RelDataType;
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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

    public Map<String, TriggerDefinition> generateTriggerDefinitionForBaseTable(Connection connection) {
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
        boolean rowTrigger = true;
        Set<RexTableInputRef.RelTableRef> tableReferences = metadataQuery.getTableReferences(relRoot.rel);
        for (RexTableInputRef.RelTableRef tableReference : tableReferences) {
            StringBuilder triggerSb = new StringBuilder();
            String tableName = tableReference.getQualifiedName().get(1);
            String triggerName = tableName + "_trig";
            String triggerFunctionName = tableName + "_trig_fn";
            List<String> primaryKeyColumnNames = getPrimaryKeyColumns(connection, tableName);
            String dropScript = "DROP TRIGGER IF EXISTS " + triggerName + " ON " + tableName + "; DROP FUNCTION IF EXISTS " + triggerFunctionName + "();";
            triggerSb.append("CREATE FUNCTION ").append(triggerFunctionName).append("() RETURNS trigger AS \n$$\nBEGIN\n");
            generateTrigger(triggerSb, normalize, tableReference.getTable(), rowTrigger, primaryKeyColumnNames, tableReferences);
            triggerSb.append("\nRETURN NEW;");
            triggerSb.append("\nEND;\n$$ LANGUAGE 'plpgsql';\n");
            triggerSb.append("CREATE TRIGGER ").append(triggerName).append(" AFTER INSERT OR UPDATE OR DELETE ON ").append(tableName);
            if (rowTrigger) {
                triggerSb.append(" FOR EACH ROW EXECUTE PROCEDURE ");
            } else {
                triggerSb.append(" FOR EACH STATEMENT EXECUTE PROCEDURE ");
            }
            triggerSb.append(triggerFunctionName).append("();");
            triggers.put(tableName, new TriggerDefinition(dropScript, triggerSb.toString()));
        }

        return triggers;
    }

    private List<String> getPrimaryKeyColumns(Connection connection, String tableName) {
        List<String> primaryKeyColumnNames = new ArrayList<>();
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(null, null, tableName)) {
            while (rs.next()) {
                primaryKeyColumnNames.add(rs.getString("COLUMN_NAME"));
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return primaryKeyColumnNames;
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

    private void generateTrigger(StringBuilder sb, Set<Term> normalizedTerms, RelOptTable sourceTable, boolean rowTrigger, List<String> primaryKeyColumnNames, Set<RexTableInputRef.RelTableRef> tableReferences) {
        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);
        SqlPrettyWriter sqlWriter = new SqlPrettyWriter(SqlPrettyWriter.config().withDialect(dialect));
        String tableName = sourceTable.getQualifiedName().get(sourceTable.getQualifiedName().size() - 1);
        String oldTableName = "_old_" + tableName;
        String newTableName = "_new_" + tableName;
        String primaryDeltaTableName = "_delta1_" + tableName;
        String secondaryDeltaTableName = "_delta2_" + tableName;
        RelDataType materializationTableRowType = materializationTable.getRowType(rexBuilder.getTypeFactory());
        List<String> materializationTableFieldNames = materializationTableRowType.getFieldNames();
        boolean joinInDelete = true;

        // DELETE
        {
            RelNode deletePrimaryDelta = relRoot.rel.accept(new DeletePrimaryDeltaVisitor(config, rexBuilder, sourceTable, primaryKeyColumnNames, materializationTable, oldTableName));
            RelNode deleteSecondaryDelta = relRoot.rel.accept(new SecondaryDeltaVisitor(config, rexBuilder, sourceTable, primaryKeyColumnNames, materializationTable, newTableName));
            deleteSecondaryDelta = deleteSecondaryDelta.accept(new DeleteSecondaryDeltaVisitor(config, rexBuilder, sourceTable, primaryKeyColumnNames, materializationTable, oldTableName, primaryDeltaTableName));

            sb.append("IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN\n");
            if (rowTrigger) {
                sb.append("\tDROP TABLE IF EXISTS ").append(oldTableName).append(";\n");
            }
            sb.append("\tDROP TABLE IF EXISTS ").append(primaryDeltaTableName).append(";\n");
            sb.append("\tDROP TABLE IF EXISTS ").append(secondaryDeltaTableName).append(";\n");
            sb.append("\tCREATE TEMPORARY TABLE ").append(oldTableName).append(" AS\n\tSELECT OLD.* FROM (VALUES(1)) _;\n");
            sb.append("\tCREATE TEMPORARY TABLE ").append(primaryDeltaTableName).append(" AS \n");

            sqlWriter.reset();
            sb.append(sqlWriter.format(relToSqlConverter.visitChild(0, deletePrimaryDelta).asSelect()));
            sb.append(";\n\n");

            sb.append("\tCREATE TEMPORARY TABLE ").append(secondaryDeltaTableName).append(" AS \n");

            sqlWriter.reset();
            sb.append(sqlWriter.format(relToSqlConverter.visitChild(0, deleteSecondaryDelta).asSelect()));
            sb.append(";\n\n");

            // Create null permutations for secondary delta
            if (tableReferences.size() > 1) {
                List<List<String>> permutations = permuteNullVariants(materializationTableFieldNames, materializationTableRowType.getFieldList(), getExpressionOrigins(relRoot.rel), tableReferences, sourceTable, true);
                if (!permutations.isEmpty()) {
                    sb.append("INSERT INTO ").append(secondaryDeltaTableName).append("(");
                    for (String fieldName : materializationTableFieldNames) {
                        sb.append(fieldName).append(',');
                    }
                    sb.setCharAt(sb.length() - 1, ')');
                    sb.append("\n\tSELECT * FROM (\n\t\t");
                    for (int i = 0; i < permutations.size(); i++) {
                        List<String> permutation = permutations.get(i);
                        if (i != 0) {
                            sb.append("\t\tUNION ALL\n\t\t");
                        }
                        sb.append("SELECT ");
                        for (String columnExpression : permutation) {
                            sb.append(columnExpression).append(", ");
                        }

                        sb.setLength(sb.length() - 2);
                        sb.append(" FROM ").append(primaryDeltaTableName).append(" a\n");
                        sb.append("\t\tWHERE ");
                        // Filter out the all null case
                        for (int j = 0; j < permutation.size(); j++) {
                            String columnExpression = permutation.get(j);
                            if (columnExpression.equals(materializationTableFieldNames.get(j))) {
                                sb.append(columnExpression).append(" IS NOT NULL OR ");
                            }
                        }
                        sb.setLength(sb.length() - " OR ".length());
                        sb.append("\n");
                    }
                    sb.append(") b;\n\n");
                }
            }

            // Primary delta
            sb.append("\tDELETE FROM ")
                .append(materializationTableName)
                .append(" a");

            if (joinInDelete) {
                sb.append(" USING ").append(primaryDeltaTableName).append(" d ");
            } else {
                sb.append(" WHERE EXISTS(SELECT 1 FROM ").append(primaryDeltaTableName).append(" d ");
            }

            sb.append("WHERE (");
            for (String fieldName : materializationTableFieldNames) {
                sb.append("a.").append(fieldName).append(',');
            }
            sb.setCharAt(sb.length() - 1, ')');
            sb.append(" IS NOT DISTINCT FROM (");
            for (String fieldName : materializationTableFieldNames) {
                sb.append("d.").append(fieldName).append(',');
            }
            sb.setCharAt(sb.length() - 1, ')');

            if (!joinInDelete) {
                sb.append(")");
            }
            sb.append(";\n\n");

            // Secondary delta
            sb.append("INSERT INTO ").append(materializationTableName).append("(");
            for (String fieldName : materializationTableFieldNames) {
                sb.append(fieldName).append(',');
            }
            sb.setCharAt(sb.length() - 1, ')');
            sb.append("\n\tSELECT ");
            sb.append("*");
            sb.append(" FROM ").append(secondaryDeltaTableName).append(" a;\n\n");

            sb.append("END IF;\n\n");
        }

        // INSERT
        {
            RelNode insertPrimaryDelta = relRoot.rel.accept(new InsertPrimaryDeltaVisitor(config, rexBuilder, sourceTable, primaryKeyColumnNames, materializationTable, newTableName, true));
            RelNode insertSecondaryDelta = relRoot.rel.accept(new SecondaryDeltaVisitor(config, rexBuilder, sourceTable, primaryKeyColumnNames, materializationTable, newTableName));
            RelNode secondaryDeltaAntiJoinBase = relRoot.rel.accept(new InsertPrimaryDeltaVisitor(config, rexBuilder, sourceTable, primaryKeyColumnNames, materializationTable, newTableName, false));
            insertSecondaryDelta = insertSecondaryDelta.accept(new InsertSecondaryDeltaVisitor(config, rexBuilder, sourceTable, primaryKeyColumnNames, materializationTable, newTableName, primaryDeltaTableName, secondaryDeltaAntiJoinBase));

            sb.append("IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN\n");
            if (rowTrigger) {
                sb.append("\tDROP TABLE IF EXISTS ").append(newTableName).append(";\n");
            }
            sb.append("\tDROP TABLE IF EXISTS ").append(primaryDeltaTableName).append(";\n");
            sb.append("\tDROP TABLE IF EXISTS ").append(secondaryDeltaTableName).append(";\n");
            sb.append("\tCREATE TEMPORARY TABLE ").append(newTableName).append(" AS\n\tSELECT NEW.* FROM (VALUES(1)) _;\n");
            sb.append("\tCREATE TEMPORARY TABLE ").append(primaryDeltaTableName).append(" AS \n");

            sqlWriter.reset();
            sb.append(sqlWriter.format(relToSqlConverter.visitChild(0, insertPrimaryDelta).asSelect()));
            sb.append(";\n\n");

            sb.append("\tCREATE TEMPORARY TABLE ").append(secondaryDeltaTableName).append(" AS \n");

            sqlWriter.reset();
            sb.append(sqlWriter.format(relToSqlConverter.visitChild(0, insertSecondaryDelta).asSelect()));
            sb.append(";\n\n");

            // Create null permutations for secondary delta
            if (tableReferences.size() > 1) {
                List<List<String>> permutations = permuteNullVariants(materializationTableFieldNames, materializationTableRowType.getFieldList(), getExpressionOrigins(relRoot.rel), tableReferences, sourceTable, false);
                if (!permutations.isEmpty()) {
                    sb.append("INSERT INTO ").append(secondaryDeltaTableName).append("(");
                    for (String fieldName : materializationTableFieldNames) {
                        sb.append(fieldName).append(',');
                    }
                    sb.setCharAt(sb.length() - 1, ')');
                    sb.append("\n\tSELECT * FROM (\n\t\t");
                    for (int i = 0; i < permutations.size(); i++) {
                        if (i != 0) {
                            sb.append("\t\tUNION ALL\n\t\t");
                        }
                        sb.append("SELECT ");
                        for (String columnExpression : permutations.get(i)) {
                            sb.append(columnExpression).append(", ");
                        }

                        sb.setLength(sb.length() - 2);
                        sb.append(" FROM ").append(primaryDeltaTableName).append(" a\n");
                    }
                    sb.append(") b;\n\n");
                }
            }

            // Primary delta
            sb.append("\tINSERT INTO ").append(materializationTableName).append("(");
            for (String fieldName : materializationTableFieldNames) {
                sb.append(fieldName).append(',');
            }
            sb.setCharAt(sb.length() - 1, ')');
            sb.append("\n\tSELECT ");
            sb.append("*");
            sb.append(" FROM ").append(primaryDeltaTableName).append(" a;\n\n");

            // Secondary delta
            sqlWriter.reset();
            String delete = sqlWriter.format(relToSqlConverter.visitChild(0, createDeleteWhereExists(config, rexBuilder, materializationTable, secondaryDeltaTableName)).asStatement());
            if (joinInDelete) {
                int existsIndex = delete.indexOf("WHERE EXISTS (");
                sb.append(delete, 0, existsIndex);
                sb.append("USING ");
                sb.append(delete, delete.indexOf(" FROM ", existsIndex) + " FROM ".length(), delete.length() - 1);
            } else {
                sb.append(delete);
            }
            sb.append(";\n\n");

            sb.append("END IF;\n");
        }
    }

    private List<Set<RelColumnOrigin>> getExpressionOrigins(RelNode relNode) {
        RelBuilder relBuilder = RelBuilder.create(config);
        relBuilder.push(relNode);
        RelMetadataQuery metadataQuery = relBuilder.getCluster().getMetadataQuery();
        int size = relNode.getRowType().getFieldCount();
        List<Set<RelColumnOrigin>> expressionLineages = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            expressionLineages.add(metadataQuery.getColumnOrigins(relNode, i));
        }
        return expressionLineages;
    }

    private List<List<String>> permuteNullVariants(List<String> projectionColumns, List<RelDataTypeField> fieldList, List<Set<RelColumnOrigin>> columnOrigins, Set<RexTableInputRef.RelTableRef> tableReferences, RelOptTable sourceTable, boolean withSourceTableOnly) {
        List<List<String>> list = new ArrayList<>(tableReferences.size());
        List<Set<RelOptTable>> nullingTablePermutations = new ArrayList<>(tableReferences.size() - 1);

        Set<RelOptTable> tablesExceptSource = new HashSet<>(tableReferences.size() - 1);
        for (RexTableInputRef.RelTableRef tableReference : tableReferences) {
            tablesExceptSource.add(tableReference.getTable());
        }
        tablesExceptSource.remove(sourceTable);
        RelOptTable[] tablesExceptSourceArray = tablesExceptSource.toArray(new RelOptTable[0]);
        if (tablesExceptSourceArray.length == 1 && withSourceTableOnly) {
            Set<RelOptTable> permutation = new HashSet<>();
            permutation.add(sourceTable);
            nullingTablePermutations.add(permutation);
        } else {
            // Start at 1 to avoid the all-null case
            for (int i = 1; i < tablesExceptSourceArray.length; i++) {
                Set<RelOptTable> permutation = new HashSet<>();
                permutation.add(sourceTable);
                for (int j = i; j < tablesExceptSourceArray.length; j++) {
                    permutation.add(tablesExceptSourceArray[j]);
                }
                nullingTablePermutations.add(permutation);
            }
        }

        SqlPrettyWriter sqlWriter = new SqlPrettyWriter(SqlPrettyWriter.config().withDialect(dialect));
        for (Set<RelOptTable> nullingTables : nullingTablePermutations) {
            List<String> permutation = new ArrayList<>(projectionColumns);
            boolean valid = false;
            for (int i = 0; i < columnOrigins.size(); i++) {
                Set<RelColumnOrigin> expressionLineage = columnOrigins.get(i);
                if (expressionLineage != null) {
                    for (RelColumnOrigin origin : expressionLineage) {
                        if (nullingTables.contains(origin.getOriginTable())) {
                            sqlWriter.reset();
                            permutation.set(i, sqlWriter.format(SqlStdOperatorTable.CAST.createCall(
                                SqlParserPos.ZERO,
                                SqlLiteral.createNull(SqlParserPos.ZERO),
                                dialect.getCastSpec(fieldList.get(i).getType())
                            )));
                            valid = true;
                            break;
                        }
                    }
                }
            }

            if (valid) {
                list.add(permutation);
            }
        }
        return list;
    }

    private RelNode createDeleteWhereExists(FrameworkConfig config, RexBuilder rexBuilder, Table materializationTable, String secondaryDeltaTableName) {
        RelBuilder relBuilder = RelBuilder.create(config);
        CalciteCatalogReader catalogReader = createCatalogReader(config, rexBuilder);
        ImmutableList<String> qualifiedMaterializationTableName = ((JdbcTable) materializationTable).tableName().names;
        RelOptTable materializationRelOptTable = catalogReader.getTableForMember(qualifiedMaterializationTableName.subList(1, qualifiedMaterializationTableName.size()));

        relBuilder.transientScan(qualifiedMaterializationTableName.get(qualifiedMaterializationTableName.size() - 1), materializationRelOptTable.getRowType());
        List<RexNode> materializationFields = relBuilder.fields();

        relBuilder.transientScan(secondaryDeltaTableName, materializationTable.getRowType(relBuilder.getTypeFactory()));

        List<RexNode> subqueryFields = relBuilder.fields(2, 1);

        relBuilder.join(JoinRelType.SEMI, relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, relBuilder.call(SqlStdOperatorTable.ROW, subqueryFields), relBuilder.call(SqlStdOperatorTable.ROW, materializationFields)));

        LogicalTableModify logicalTableModify = LogicalTableModify.create(materializationRelOptTable, catalogReader, relBuilder.build(),
                                                                          LogicalTableModify.Operation.DELETE, null, null, false);
        return logicalTableModify;
    }

    private static CalciteCatalogReader createCatalogReader(FrameworkConfig config, RexBuilder rexBuilder) {
        final SchemaPlus rootSchema = rootSchema(config.getDefaultSchema());

        return new CalciteCatalogReader(
            CalciteSchema.from(rootSchema),
            CalciteSchema.from(config.getDefaultSchema()).path(null),
            rexBuilder.getTypeFactory(), connConfig(config));
    }

    private static CalciteConnectionConfig connConfig(FrameworkConfig c) {
        CalciteConnectionConfigImpl config =
            c.getContext().unwrap(CalciteConnectionConfigImpl.class);
        if (config == null) {
            config = new CalciteConnectionConfigImpl(new Properties());
        }
        if (!config.isSet(CalciteConnectionProperty.CASE_SENSITIVE)) {
            config = config.set(CalciteConnectionProperty.CASE_SENSITIVE,
                                String.valueOf(c.getParserConfig().caseSensitive()));
        }
        if (!config.isSet(CalciteConnectionProperty.CONFORMANCE)) {
            config = config.set(CalciteConnectionProperty.CONFORMANCE,
                                String.valueOf(c.getParserConfig().conformance()));
        }
        return config;
    }

    private static SchemaPlus rootSchema(SchemaPlus schema) {
        for (;;) {
            if (schema.getParentSchema() == null) {
                return schema;
            }
            schema = schema.getParentSchema();
        }
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

    public static RelNode adaptProjectType(LogicalProject logicalProject, RelNode newInput) {
        List<RexNode> projects = new ArrayList<>(logicalProject.getProjects().size());
        List<RelDataTypeField> projectFieldList = logicalProject.getRowType().getFieldList();
        List<RelDataTypeField> fieldList = newInput.getRowType().getFieldList();
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


        return logicalProject.copy(logicalProject.getTraitSet(), newInput, projects, new RelRecordType(projectTypeList));
    }

    private abstract static class PrimaryDeltaVisitor extends RelShuttleImpl {

        protected final RelOptTable sourceTable;
        protected final List<String> sourceTablePrimaryKeyColumnNames;
        protected final Table materializationTable;
        protected final String transitionTableName;
        protected FrameworkConfig config;
        protected RexBuilder rexBuilder;

        public PrimaryDeltaVisitor(FrameworkConfig config, RexBuilder rexBuilder, RelOptTable sourceTable, List<String> sourceTablePrimaryKeyColumnNames, Table materializationTable, String transitionTableName) {
            this.sourceTable = sourceTable;
            this.sourceTablePrimaryKeyColumnNames = sourceTablePrimaryKeyColumnNames;
            this.materializationTable = materializationTable;
            this.transitionTableName = transitionTableName;
            this.config = config;
            this.rexBuilder = rexBuilder;
        }

        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            stack.push(parent);
            try {
                RelNode child2 = child.accept(this);
                if (child2 != child) {
                    if (parent instanceof LogicalProject) {
                        return adaptProjectType((LogicalProject) parent, child2);
                    } else {
                        final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
                        newInputs.set(i, child2);
                        return parent.copy(parent.getTraitSet(), newInputs);
                    }
                }
                return parent;
            } finally {
                stack.pop();
            }
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            // The most important step here is to move the table T to the left
            // In addition, we rewrite joins to exclude null-extended tuples

            // For vD we only care about non-null extending tuples, so we transform joins to exclude null extended tuples
            if (sourceTable.equals(join.getLeft().getTable())) {
                JoinRelType joinType = forExcludeRightNullExtended(join.getJoinType());
                return join.copy(join.getTraitSet(), join.getCondition(), correlateJoinWithTransitionTable(join.getLeft()), join.getRight(), joinType, join.isSemiJoinDone());
            } else if (sourceTable.equals(join.getRight().getTable())) {
                return swap(join.copy(join.getTraitSet(), join.getCondition(), join.getLeft(), correlateJoinWithTransitionTable(join.getRight()), join.getJoinType(), join.isSemiJoinDone()));
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

        protected abstract RelNode correlateJoinWithTransitionTable(RelNode node);

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
                relNode = adaptProjectType((LogicalProject) relNode, join);
            } else {
                relNode = join;
            }
            return relNode;
        }

    }

    private static class DeletePrimaryDeltaVisitor extends PrimaryDeltaVisitor {

        public DeletePrimaryDeltaVisitor(FrameworkConfig config, RexBuilder rexBuilder, RelOptTable sourceTable, List<String> primaryKeyColumnNames, Table materializationTable, String transitionTableName) {
            super(config, rexBuilder, sourceTable, primaryKeyColumnNames, materializationTable, transitionTableName);
        }

        @Override
        protected RelNode correlateJoinWithTransitionTable(RelNode node) {
            RelBuilder relBuilder = RelBuilder.create(config);
            return relBuilder.transientScan(transitionTableName, node.getRowType()).build();
        }
    }

    private static class InsertPrimaryDeltaVisitor extends PrimaryDeltaVisitor {

        private final boolean equal;

        public InsertPrimaryDeltaVisitor(FrameworkConfig config, RexBuilder rexBuilder, RelOptTable sourceTable, List<String> primaryKeyColumnNames, Table materializationTable, String transitionTableName, boolean equal) {
            super(config, rexBuilder, sourceTable, primaryKeyColumnNames, materializationTable, transitionTableName);
            this.equal = equal;
        }

        @Override
        protected RelNode correlateJoinWithTransitionTable(RelNode node) {
            RelBuilder relBuilder = RelBuilder.create(config);
            List<RexNode> nodeFields = new ArrayList<>(sourceTablePrimaryKeyColumnNames.size());
            List<RexNode> transitionFields = new ArrayList<>(sourceTablePrimaryKeyColumnNames.size());
            relBuilder.push(node);
            for (String columnName : sourceTablePrimaryKeyColumnNames) {
                nodeFields.add(relBuilder.field(1, 0, columnName));
            }
            relBuilder.transientScan(transitionTableName, node.getRowType());
            for (String columnName : sourceTablePrimaryKeyColumnNames) {
                transitionFields.add(relBuilder.field(2, 1, columnName));
            }

            relBuilder.join(JoinRelType.INNER, relBuilder.call(equal ? SqlStdOperatorTable.IS_NOT_DISTINCT_FROM : SqlStdOperatorTable.IS_DISTINCT_FROM, relBuilder.call(SqlStdOperatorTable.ROW, nodeFields), relBuilder.call(SqlStdOperatorTable.ROW, transitionFields)));

            List<RexNode> fields = new ArrayList<>();
            for (RelDataTypeField field : node.getRowType().getFieldList()) {
                fields.add(relBuilder.field(field.getName()));
            }

            relBuilder.project(fields);
            return relBuilder.build();
        }
    }

    private static class SecondaryDeltaVisitor extends PrimaryDeltaVisitor {

        public SecondaryDeltaVisitor(FrameworkConfig config, RexBuilder rexBuilder, RelOptTable sourceTable, List<String> sourceTablePrimaryKeyColumnNames, Table materializationTable, String transitionTableName) {
            super(config, rexBuilder, sourceTable, sourceTablePrimaryKeyColumnNames, materializationTable, transitionTableName);
        }

        @Override
        protected RelNode correlateJoinWithTransitionTable(RelNode node) {
            return node;
        }
    }

    private static class DeleteSecondaryDeltaVisitor extends PrimaryDeltaVisitor {

        private final String primaryDeltaTableName;
        private boolean root = true;

        public DeleteSecondaryDeltaVisitor(FrameworkConfig config, RexBuilder rexBuilder, RelOptTable table, List<String> primaryKeyColumnNames, Table materializationTable, String transitionTableName, String primaryDeltaTableName) {
            super(config, rexBuilder, table, primaryKeyColumnNames, materializationTable, transitionTableName);
            this.primaryDeltaTableName = primaryDeltaTableName;
        }

        @Override
        protected RelNode correlateJoinWithTransitionTable(RelNode node) {
            return node;
        }

        @Override
        public RelNode visit(LogicalProject project) {
            boolean original = root;
            root = false;
            RelNode node = super.visit(project);
            if (original) {
                LogicalProject logicalProject = (LogicalProject) node;
                LogicalFilter filter;
                if (logicalProject.getInput() instanceof LogicalFilter) {
                    filter = (LogicalFilter) logicalProject.getInput();
                } else {
                    filter = LogicalFilter.create(logicalProject.getInput(), RexUtil.composeConjunction(rexBuilder, Collections.emptyList()));
                }

                RelBuilder relBuilder = RelBuilder.create(config);
                RelDataType materializationRowType = materializationTable.getRowType(rexBuilder.getTypeFactory());
                List<RelDataTypeField> materializationFieldList = materializationRowType.getFieldList();
                List<RexNode> nodeFields = new ArrayList<>(materializationFieldList.size());
                List<RexNode> deltaFields = new ArrayList<>(materializationFieldList.size());
                relBuilder.push(filter.getInput());
                nodeFields.addAll(logicalProject.getProjects());
                relBuilder.transientScan(primaryDeltaTableName, materializationRowType);
                for (RelDataTypeField field : materializationFieldList) {
                    deltaFields.add(relBuilder.field(2, 1, field.getName()));
                }

                relBuilder.join(JoinRelType.INNER, relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, relBuilder.call(SqlStdOperatorTable.ROW, nodeFields), relBuilder.call(SqlStdOperatorTable.ROW, deltaFields)));
                int filterFieldSize = filter.getRowType().getFieldList().size();
                List<RexNode> filterFields = new ArrayList<>(filterFieldSize);
                for (int i = 0; i < filterFieldSize; i++) {
                    filterFields.add(relBuilder.field(i));
                }
                relBuilder.project(filterFields);

                List<RexNode> preFields = new ArrayList<>(sourceTablePrimaryKeyColumnNames.size());
                List<RexNode> mainQueryFields = new ArrayList<>(sourceTablePrimaryKeyColumnNames.size());
                RelMetadataQuery metadataQuery = relBuilder.getCluster().getMetadataQuery();
                RelNode currentRelNode = relBuilder.peek();
                int size = currentRelNode.getRowType().getFieldCount();
                List<Set<RexNode>> expressionLineages = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    expressionLineages.add(metadataQuery.getExpressionLineage(currentRelNode, relBuilder.field(i)));
                }
                for (String columnName : sourceTablePrimaryKeyColumnNames) {
                    int sourceColumnIndex = -1;
                    OUTER: for (int i = 0; i < expressionLineages.size(); i++) {
                        Set<RexNode> expressionLineage = expressionLineages.get(i);
                        if (expressionLineage != null) {
                            for (RexNode rexNode : expressionLineage) {
                                if (rexNode instanceof RexTableInputRef) {
                                    RexTableInputRef ref = (RexTableInputRef) rexNode;
                                    if (ref.getTableRef().getTable().equals(sourceTable) && columnName.equals(sourceTable.getRowType().getFieldNames().get(ref.getIndex()))) {
                                        sourceColumnIndex = i;
                                        break OUTER;
                                    }
                                }
                            }
                        }
                    }

                    mainQueryFields.add(relBuilder.field(sourceColumnIndex));
                }
                relBuilder.scan(sourceTable.getQualifiedName());
                for (String columnName : sourceTablePrimaryKeyColumnNames) {
                    preFields.add(relBuilder.field(2, 1, columnName));
                }
                RexNode antiJoinCondition = relBuilder.call(SqlStdOperatorTable.IS_DISTINCT_FROM, relBuilder.call(SqlStdOperatorTable.ROW, preFields), relBuilder.call(SqlStdOperatorTable.ROW, mainQueryFields));
                relBuilder.antiJoin(antiJoinCondition);

                List<RexNode> newProjects = new ArrayList<>(logicalProject.getProjects().size());
                List<RexNode> projects = logicalProject.getProjects();
                for (int i = 0; i < projects.size(); i++) {
                    newProjects.add(relBuilder.alias(projects.get(i), logicalProject.getRowType().getFieldNames().get(i)));
                }

                relBuilder.project(newProjects);

                node = relBuilder.build();
            }
            return node;
        }

    }

    private static class InsertSecondaryDeltaVisitor extends RelShuttleImpl {

        protected final RelOptTable sourceTable;
        protected final List<String> sourceTablePrimaryKeyColumnNames;
        protected final Table materializationTable;
        protected final String transitionTableName;
        protected FrameworkConfig config;
        protected RexBuilder rexBuilder;
        private final String primaryDeltaTableName;
        private final RelNode secondaryDeltaAntiJoinBase;
        private boolean root = true;

        public InsertSecondaryDeltaVisitor(FrameworkConfig config, RexBuilder rexBuilder, RelOptTable table, List<String> primaryKeyColumnNames, Table materializationTable, String transitionTableName, String primaryDeltaTableName, RelNode secondaryDeltaAntiJoinBase) {
            this.sourceTable = table;
            this.sourceTablePrimaryKeyColumnNames = primaryKeyColumnNames;
            this.materializationTable = materializationTable;
            this.transitionTableName = transitionTableName;
            this.config = config;
            this.rexBuilder = rexBuilder;
            this.primaryDeltaTableName = primaryDeltaTableName;
            this.secondaryDeltaAntiJoinBase = secondaryDeltaAntiJoinBase;
        }

        @Override
        public RelNode visit(LogicalProject project) {
            boolean original = root;
            root = false;
            RelNode node = super.visit(project);
            if (original) {
                LogicalProject logicalProject = (LogicalProject) node;
                LogicalFilter filter;
                if (logicalProject.getInput() instanceof LogicalFilter) {
                    filter = (LogicalFilter) logicalProject.getInput();
                } else {
                    filter = LogicalFilter.create(logicalProject.getInput(), RexUtil.composeConjunction(rexBuilder, Collections.emptyList()));
                }

                RelBuilder relBuilder = RelBuilder.create(config);
                RelDataType materializationRowType = materializationTable.getRowType(rexBuilder.getTypeFactory());
                List<RelDataTypeField> materializationFieldList = materializationRowType.getFieldList();
                List<RexNode> nodeFields = new ArrayList<>(materializationFieldList.size());
                List<RexNode> deltaFields = new ArrayList<>(materializationFieldList.size());
                relBuilder.push(filter.getInput());
                nodeFields.addAll(logicalProject.getProjects());
                relBuilder.transientScan(primaryDeltaTableName, materializationRowType);
                for (RelDataTypeField field : materializationFieldList) {
                    deltaFields.add(relBuilder.field(2, 1, field.getName()));
                }

                relBuilder.join(JoinRelType.INNER, relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, relBuilder.call(SqlStdOperatorTable.ROW, nodeFields), relBuilder.call(SqlStdOperatorTable.ROW, deltaFields)));
                int filterFieldSize = filter.getRowType().getFieldList().size();
                List<RexNode> filterFields = new ArrayList<>(filterFieldSize);
                for (int i = 0; i < filterFieldSize; i++) {
                    filterFields.add(relBuilder.field(i));
                }
                relBuilder.project(filterFields);

                List<RexNode> preFields = new ArrayList<>(sourceTablePrimaryKeyColumnNames.size());
                List<RexNode> mainQueryFields = new ArrayList<>(sourceTablePrimaryKeyColumnNames.size());
                RelMetadataQuery metadataQuery = relBuilder.getCluster().getMetadataQuery();
                RelNode currentRelNode = relBuilder.peek();
                int size = currentRelNode.getRowType().getFieldCount();
                List<Set<RexNode>> expressionLineages = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    expressionLineages.add(metadataQuery.getExpressionLineage(currentRelNode, relBuilder.field(i)));
                }
                for (String columnName : sourceTablePrimaryKeyColumnNames) {
                    int sourceColumnIndex = -1;
                    OUTER: for (int i = 0; i < expressionLineages.size(); i++) {
                        Set<RexNode> expressionLineage = expressionLineages.get(i);
                        if (expressionLineage != null) {
                            for (RexNode rexNode : expressionLineage) {
                                if (rexNode instanceof RexTableInputRef) {
                                    RexTableInputRef ref = (RexTableInputRef) rexNode;
                                    if (ref.getTableRef().getTable().equals(sourceTable) && columnName.equals(sourceTable.getRowType().getFieldNames().get(ref.getIndex()))) {
                                        sourceColumnIndex = i;
                                        break OUTER;
                                    }
                                }
                            }
                        }
                    }

                    mainQueryFields.add(relBuilder.field(sourceColumnIndex));
                }
                RelNode antiJoinBaseNode = secondaryDeltaAntiJoinBase.getInput(0);
                if (antiJoinBaseNode instanceof LogicalFilter) {
                    relBuilder.push(antiJoinBaseNode.getInput(0));
                } else {
                    relBuilder.push(antiJoinBaseNode);
                }
                for (String columnName : sourceTablePrimaryKeyColumnNames) {
                    preFields.add(relBuilder.field(2, 1, columnName));
                }
                RexNode antiJoinCondition = relBuilder.call(SqlStdOperatorTable.IS_DISTINCT_FROM, relBuilder.call(SqlStdOperatorTable.ROW, preFields), relBuilder.call(SqlStdOperatorTable.ROW, mainQueryFields));
                if (antiJoinBaseNode instanceof LogicalFilter) {
                    antiJoinCondition = relBuilder.and(((LogicalFilter) antiJoinBaseNode).getCondition(), antiJoinCondition);
                }
                relBuilder.antiJoin(antiJoinCondition);
                List<RexNode> newProjects = new ArrayList<>(logicalProject.getProjects().size());
                List<RexNode> projects = logicalProject.getProjects();
                for (int i = 0; i < projects.size(); i++) {
                    newProjects.add(relBuilder.alias(projects.get(i), logicalProject.getRowType().getFieldNames().get(i)));
                }

                relBuilder.project(newProjects);

                node = relBuilder.build();
            }
            return node;
        }

    }
}

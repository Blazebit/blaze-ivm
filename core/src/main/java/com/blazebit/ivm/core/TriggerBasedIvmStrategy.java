package com.blazebit.ivm.core;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
public class TriggerBasedIvmStrategy {

//    private final String viewQuery;
//    private SqlParser parser
    private SqlNode viewQuery;

    public TriggerBasedIvmStrategy(String viewSqlQuery) {
//        this.viewSqlCQuery = viewSqlQuery;
        try {
            this.viewQuery = SqlParser.create(viewSqlQuery).parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    public String generateTriggerDefinitionForBaseTable(String tablename) {
        // produce join-disjunctive normal form
        generateJoinDisjunctiveNormalForm((SqlSelect) viewQuery);
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
        return null;
    }

    private Set<Term> generateJoinDisjunctiveNormalForm(SqlSelect select) {
        SqlJoin parentJoinNode = retrieveParentJoinNode(select);
        return normalize(parentJoinNode);
    }

    private SqlJoin retrieveParentJoinNode(SqlSelect select) {
        return (SqlJoin) select.getFrom();
    }

    private SqlNode retrieveParentPredicateNode(SqlSelect select) {

    }

    private Set<Term> normalize(SqlNode parentJoinNode) {
        String table;
        String alias = null;
        Set<Term> result;
        switch (parentJoinNode.getKind()) {
            case AS:
                table = ((SqlCall) parentJoinNode).getOperandList().get(0).toString();
                alias = ((SqlCall) parentJoinNode).getOperandList().get(1).toString();
                result = new HashSet<>();
                result.add(new Term(Collections.singleton(table), null));
                return result;
            case IDENTIFIER:
                table = parentJoinNode.toString();
                result = new HashSet<>();
                result.add(new Term(Collections.singleton(table), null));
                return result;
            case SELECT:
                SqlJoin nestedParentJoinNode = retrieveParentJoinNode((SqlSelect) parentJoinNode);
                SqlNode nestedPredicate = retrieveParentPredicateNode((SqlSelect) parentJoinNode);
                Set<Term> nestedTerms = normalize(nestedParentJoinNode);
                Set<String> nullRejectedTabes = getNullRejectedTables(nestedPredicate);
                Iterator<Term> nestedTermsIter = nestedTerms.iterator();
                while (nestedTermsIter.hasNext()) {
                    Term nestedTerm = nestedTermsIter.next();
                    if (nestedTerm.getTables().containsAll(nullRejectedTabes)) {
                        nestedTerm.addPredicateConjunct(nestedPredicate);
                    } else {
                        nestedTermsIter.remove();
                    }
                }
                return nestedTerms;
            case JOIN:

        }
    }

    private Set<String> getNullRejectedTables(SqlNode predicate) {

    }
}

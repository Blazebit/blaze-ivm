package com.blazebit.ivm.testsuite;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
public interface RelationalModelAccessor {

    public String tableFromEntity(Class<?> entityClass);

    public String tableFromEntityRelation(Class<?> entityClass, String relationName);
}

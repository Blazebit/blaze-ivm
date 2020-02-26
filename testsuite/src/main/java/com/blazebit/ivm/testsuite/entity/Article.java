package com.blazebit.ivm.testsuite.entity;

import javax.persistence.Entity;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
@Entity
public class Article extends LongSequenceEntity {
    private String name;

    public Article() {
    }

    public Article(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

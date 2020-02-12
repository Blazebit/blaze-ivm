package com.blazebit.ivm.testsuite.entity;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import java.io.Serializable;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
@MappedSuperclass
public class LongSequenceEntity implements Serializable {

    private Long id;

    public LongSequenceEntity() {
    }

    public LongSequenceEntity(Long id) {
        this.id = id;
    }

    @Id
    @GeneratedValue
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LongSequenceEntity)) {
            return false;
        }
        LongSequenceEntity that = (LongSequenceEntity) o;
        return getId() != null ? getId().equals(that.getId()) : that.getId() == null;
    }

    @Override
    public int hashCode() {
        return getId() != null ? getId().hashCode() : 0;
    }
}

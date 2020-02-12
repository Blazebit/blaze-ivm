package com.blazebit.ivm.testsuite.entity;

import javax.persistence.Entity;
import javax.persistence.OneToMany;
import javax.persistence.OrderColumn;
import javax.persistence.Table;
import java.util.List;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
@Entity
@Table(name = "_order")
public class Order extends LongSequenceEntity {
    private List<OrderPosition> orderPositions;

    @OneToMany
    @OrderColumn(name = "sort_index")
    public List<OrderPosition> getOrderPositions() {
        return orderPositions;
    }

    public void setOrderPositions(List<OrderPosition> orderPositions) {
        this.orderPositions = orderPositions;
    }
}

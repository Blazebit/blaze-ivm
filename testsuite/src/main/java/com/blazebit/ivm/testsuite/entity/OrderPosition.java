package com.blazebit.ivm.testsuite.entity;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Objects;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
@Entity
@Table(name = "order_position")
public class OrderPosition {
    private Id id = new Id();
    private Order order;
    private Article article;
    private Integer sortIndex;
    private Integer amount;

    @EmbeddedId
    public Id getId() {
        return id;
    }

    public void setId(Id id) {
        this.id = id;
    }

    @ManyToOne
    @JoinColumn(name = "order_id", insertable = false, updatable = false)
    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
        if (order != null) {
            id.setOrderId(order.getId());
        }
    }

    @ManyToOne
    @JoinColumn(name = "article_id", insertable = false, updatable = false)
    public Article getArticle() {
        return article;
    }

    public void setArticle(Article article) {
        this.article = article;
        if (article != null) {
            id.setArticleId(article.getId());
        }
    }

    public Integer getSortIndex() {
        return sortIndex;
    }

    public void setSortIndex(Integer sortIndex) {
        this.sortIndex = sortIndex;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    @Embeddable
    public static class Id implements Serializable {
        private Long articleId;
        private Long orderId;

        @Column(name = "article_id")
        public Long getArticleId() {
            return articleId;
        }

        public void setArticleId(Long articleId) {
            this.articleId = articleId;
        }

        @Column(name = "order_id")
        public Long getOrderId() {
            return orderId;
        }

        public void setOrderId(Long orderId) {
            this.orderId = orderId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Id id = (Id) o;
            return Objects.equals(articleId, id.articleId) &&
                    Objects.equals(orderId, id.orderId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(articleId, orderId);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderPosition that = (OrderPosition) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}

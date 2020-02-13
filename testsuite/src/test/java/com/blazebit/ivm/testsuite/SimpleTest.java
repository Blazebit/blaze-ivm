package com.blazebit.ivm.testsuite;

import com.blazebit.ivm.testsuite.entity.Article;
import com.blazebit.ivm.testsuite.entity.Order;
import com.blazebit.ivm.testsuite.entity.OrderPosition;
import org.junit.Test;

/**
 *
 * @author Moritz Becker
 * @author Christian Beikov
 * @since 1.0.0
 */
public class SimpleTest extends MaterializationTest {

    @Override
    protected Class<?>[] getEntityClasses() {
        return new Class[] {
                Article.class,
                Order.class,
                OrderPosition.class
        };
    }

    @Test
    public void simpleTest() {
        // Given
        Order order = new Order();
        Article article = new Article();
        article.setName("Article 1");
        em.persist(order);
        em.persist(article);
        OrderPosition orderPosition = new OrderPosition();
        orderPosition.setArticle(article);
        orderPosition.setOrder(order);
        em.persist(orderPosition);
        em.flush();

        // create view
        String viewQuery = "SELECT art.id as art_id, ord.id as ord_id, art.name, ordpos.amount FROM _order ord " +
            "LEFT JOIN order_position ordpos ON ordpos.order_id = ord.id " +
            "LEFT JOIN article art ON art.id = ordpos.article_id " +
            "WHERE ord.id > 0";
        setupMaterialization(viewQuery);

        // When
        Order newOrder = new Order();
        em.persist(newOrder);
        em.flush();

        // Then
        assertMaterializationEqual();
    }
}

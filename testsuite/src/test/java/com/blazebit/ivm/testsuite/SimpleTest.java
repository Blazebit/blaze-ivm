package com.blazebit.ivm.testsuite;

import com.blazebit.ivm.core.TriggerBasedIvmStrategy;
import com.blazebit.ivm.testsuite.entity.Article;
import com.blazebit.ivm.testsuite.entity.Order;
import com.blazebit.ivm.testsuite.entity.OrderPosition;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author Moritz Becker
 * @since 1.0.0
 */
public class SimpleTest extends AbstractHibernatePersistenceTest {
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

        // create view
        String viewQuery = "SELECT art.id as art_id, ord.id as ord_id, art.name, ordpos.amount FROM _order ord " +
                "LEFT JOIN order_position ordpos ON ordpos.order_id = ord.id " +
                "LEFT JOIN article art ON art.id = ordpos.article_id";
        em.createNativeQuery("CREATE MATERIALIZED VIEW TEST_VIEW AS " + viewQuery).executeUpdate();
        TriggerBasedIvmStrategy triggerBasedIvmStrategy = new TriggerBasedIvmStrategy(viewQuery);
        triggerBasedIvmStrategy.generateTriggerDefinitionForBaseTable("ordpos");

        // When
        Order newOrder = new Order();
        em.persist(newOrder);

        // Then
        List<?> actualResult = em.createNativeQuery("SELECT * FROM TEST_VIEW").getResultList();
        em.createNativeQuery("REFRESH MATERIALIZED VIEW TEST_VIEW").executeUpdate();
        List<?> expectedResult = em.createNativeQuery("SELECT * FROM TEST_VIEW").getResultList();
        assertEquals(expectedResult, actualResult);
    }
}

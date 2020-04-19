/*
 * Copyright 2020 - 2020 Blazebit.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blazebit.ivm.testsuite;

import com.blazebit.ivm.core.TriggerBasedIvmStrategy;
import com.blazebit.ivm.testsuite.entity.Article;
import com.blazebit.ivm.testsuite.entity.Order;
import com.blazebit.ivm.testsuite.entity.OrderPosition;
import org.junit.Test;

import java.util.Map;

/**
 *
 * @author Moritz Becker
 * @author Christian Beikov
 * @since 1.0.0
 */
public class ThreeLevelTest extends MaterializationTest {

    private static final String viewQuery = "SELECT art.id as art_id, ord.id as ord_id, art.name, ordpos.amount FROM _order ord " +
        "LEFT JOIN order_position ordpos ON ordpos.order_id = ord.id " +
        "LEFT JOIN article art ON art.id = ordpos.article_id " +
        "WHERE ord.id > 0";

    @Override
    protected Class<?>[] getEntityClasses() {
        return new Class[] {
                Article.class,
                Order.class,
                OrderPosition.class
        };
    }

    @Test
    public void insertRootTest() {
        // Given
        Order order = new Order();
        Article article = new Article("Article 1");
        em.persist(order);
        em.persist(article);
        OrderPosition orderPosition = new OrderPosition(order, article);
        em.persist(orderPosition);
        em.flush();

        // create view
        Map<String, TriggerBasedIvmStrategy.TriggerDefinition> triggerDefinitions = setupMaterialization(viewQuery);

        // When
        Order newOrder = new Order();
        em.persist(newOrder);
        em.flush();

        // Then
        assertMaterializationEqual();
    }

    @Test
    public void deleteEmptyRootTest() {
        // Given
        Order order1 = new Order();
        Order order2 = new Order();
        Article article = new Article("Article 1");
        em.persist(order1);
        em.persist(order2);
        em.persist(article);
        OrderPosition orderPosition = new OrderPosition(order1, article);
        em.persist(orderPosition);
        em.flush();

        // create view
        Map<String, TriggerBasedIvmStrategy.TriggerDefinition> triggerDefinitions = setupMaterialization(viewQuery);

        // When
        em.remove(order2);
        em.flush();

        // Then
        assertMaterializationEqual();
    }

    @Test
    public void deleteRootTest() {
        // Given
        Order order1 = new Order();
        Order order2 = new Order();
        Article article = new Article("Article 1");
        em.persist(order1);
        em.persist(order2);
        em.persist(article);
        OrderPosition orderPosition = new OrderPosition(order1, article);
        em.persist(orderPosition);
        OrderPosition orderPosition2 = new OrderPosition(order2, article);
        em.persist(orderPosition2);
        em.flush();

        // create view
        Map<String, TriggerBasedIvmStrategy.TriggerDefinition> triggerDefinitions = setupMaterialization(viewQuery);

        // When
        em.remove(orderPosition2);
        em.remove(order2);
        em.flush();

        // Then
        assertMaterializationEqual();
    }

    @Test
    public void deleteSubToEmptyTest() {
        // Given
        Order order1 = new Order();
        Order order2 = new Order();
        Article article = new Article("Article 1");
        em.persist(order1);
        em.persist(order2);
        em.persist(article);
        OrderPosition orderPosition = new OrderPosition(order1, article);
        em.persist(orderPosition);
        OrderPosition orderPosition2 = new OrderPosition(order2, article);
        em.persist(orderPosition2);
        em.flush();

        // create view
        Map<String, TriggerBasedIvmStrategy.TriggerDefinition> triggerDefinitions = setupMaterialization(viewQuery);

        // When
        em.remove(orderPosition2);
        em.flush();

        // Then
        assertMaterializationEqual();
    }

    @Test
    public void insertSubToEmptyTest() {
        // Given
        Order order1 = new Order();
        Order order2 = new Order();
        Article article1 = new Article("Article 1");
        em.persist(order1);
        em.persist(order2);
        em.persist(article1);
        OrderPosition orderPosition = new OrderPosition(order1, article1);
        em.persist(orderPosition);
        em.flush();

        // create view
        Map<String, TriggerBasedIvmStrategy.TriggerDefinition> triggerDefinitions = setupMaterialization(viewQuery);

        // When
        em.persist(new OrderPosition(order2, article1));
        em.flush();

        // Then
        assertMaterializationEqual();
    }

    @Test
    public void insertSubTest() {
        // Given
        Order order1 = new Order();
        Order order2 = new Order();
        Article article1 = new Article("Article 1");
        Article article2 = new Article("Article 2");
        em.persist(order1);
        em.persist(order2);
        em.persist(article1);
        em.persist(article2);
        OrderPosition orderPosition = new OrderPosition(order1, article1);
        em.persist(orderPosition);
        OrderPosition orderPosition2 = new OrderPosition(order2, article1);
        em.persist(orderPosition2);
        em.flush();

        // create view
        Map<String, TriggerBasedIvmStrategy.TriggerDefinition> triggerDefinitions = setupMaterialization(viewQuery);

        // When
        em.persist(new OrderPosition(order2, article2));
        em.flush();

        // Then
        assertMaterializationEqual();
    }
}

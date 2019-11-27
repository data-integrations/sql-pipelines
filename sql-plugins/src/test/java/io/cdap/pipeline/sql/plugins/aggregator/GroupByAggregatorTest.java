/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.pipeline.sql.plugins.aggregator;

import io.cdap.pipeline.sql.api.template.QueryContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class GroupByAggregatorTest {
  @Test
  public void testEmptyAggregates() {
    GroupByConfig config = new GroupByConfig("a", "");
    Assert.assertEquals(0, config.getAggregates().size());
  }

  @Test
  public void testEmptyGroupByFields() {
    GroupByConfig config = new GroupByConfig("", "a:COUNT(b)");
    Assert.assertEquals(0, config.getGroupByFields().size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidGroupByField() {
    GroupByConfig config = new GroupByConfig(",", "a:COUNT(b)");
    config.getGroupByFields();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoAliasAggregate() {
    GroupByConfig config = new GroupByConfig("", ":COUNT(b)");
    config.getAggregates();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoFieldAggregate() {
    GroupByConfig config = new GroupByConfig("", "a:COUNT()");
    config.getAggregates();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testImproperFormatAggregate() {
    GroupByConfig config = new GroupByConfig("", "a:COUNT(a");
    config.getAggregates();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAggregatesFieldFunction() {
    GroupByConfig config = new GroupByConfig("", "a:invalid(b)");
    GroupByAggregator aggregator = new GroupByAggregator(config);
    RelBuilder builder = Mockito.mock(RelBuilder.class);
    QueryContext context = new QueryContext(builder, null);
    aggregator.getQuery(context);
  }

  @Test
  public void testValidCountStar() {
    GroupByConfig config = new GroupByConfig("", "d:COUNT(*)");
    GroupByAggregator aggregator = new GroupByAggregator(config);

    RelBuilder builder = Mockito.mock(RelBuilder.class);
    RelBuilder.AggCall aggCall = Mockito.mock(RelBuilder.AggCall.class);
    RelBuilder finalBuilder = Mockito.mock(RelBuilder.class);
    RelNode expected = Mockito.mock(RelNode.class);

    Mockito.when(builder.countStar("d")).thenReturn(aggCall);
    List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
    aggCalls.add(aggCall);
    Mockito.when(builder.aggregate(null, aggCalls)).thenReturn(finalBuilder);
    Mockito.when(finalBuilder.build()).thenReturn(expected);

    QueryContext context = new QueryContext(builder, null);
    RelNode rel = aggregator.getQuery(context);
    Assert.assertEquals(expected, rel);
  }
}

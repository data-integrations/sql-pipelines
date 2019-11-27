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

package io.cdap.pipeline.sql.plugins.filter;

import io.cdap.pipeline.sql.api.template.QueryContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class FilterSQLTransformTest {
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyLeftValue() {
    FilterSQLTransform.FilterTransformTransformConfig config = new FilterSQLTransform.FilterTransformTransformConfig(
      "", "b", "c", "d", "e");
    FilterSQLTransform transform = new FilterSQLTransform(config);
    transform.getQuery(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyRightValue() {
    FilterSQLTransform.FilterTransformTransformConfig config = new FilterSQLTransform.FilterTransformTransformConfig(
      "a", "b", "", "d", "e");
    FilterSQLTransform transform = new FilterSQLTransform(config);
    transform.getQuery(null);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testEmptyLeftType() {
    FilterSQLTransform.FilterTransformTransformConfig config = new FilterSQLTransform.FilterTransformTransformConfig(
      "a", "", "c", "d", "e");
    FilterSQLTransform transform = new FilterSQLTransform(config);
    transform.getQuery(null);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testEmptyRightType() {
    FilterSQLTransform.FilterTransformTransformConfig config = new FilterSQLTransform.FilterTransformTransformConfig(
      "a", "b", "c", "", "e");
    FilterSQLTransform transform = new FilterSQLTransform(config);
    transform.getQuery(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyOperationType() {
    FilterSQLTransform.FilterTransformTransformConfig config = new FilterSQLTransform.FilterTransformTransformConfig(
      "a", "b", "c", "d", "");
    FilterSQLTransform transform = new FilterSQLTransform(config);
    transform.getQuery(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidOperandType() {
    FilterSQLTransform.FilterTransformTransformConfig config = new FilterSQLTransform.FilterTransformTransformConfig(
      "a", "b", "c", "d", "e");
    FilterSQLTransform transform = new FilterSQLTransform(config);
    RelBuilder builder = Mockito.mock(RelBuilder.class);
    QueryContext context = new QueryContext(builder, null);
    transform.getQuery(context);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidOperationType() {
    FilterSQLTransform.FilterTransformTransformConfig config = new FilterSQLTransform.FilterTransformTransformConfig(
      "a", "string", "c", "string", "e");
    FilterSQLTransform transform = new FilterSQLTransform(config);
    RelBuilder builder = Mockito.mock(RelBuilder.class);
    QueryContext context = new QueryContext(builder, null);
    transform.getQuery(context);
  }

  @Test
  public void testValidFilter() {
    FilterSQLTransform.FilterTransformTransformConfig config = new FilterSQLTransform.FilterTransformTransformConfig(
      "a", "field", "c", "string", "equals");
    FilterSQLTransform transform = new FilterSQLTransform(config);

    RelBuilder builder = Mockito.mock(RelBuilder.class);
    RexInputRef aField = Mockito.mock(RexInputRef.class);
    RexNode cValue = Mockito.mock(RexNode.class);
    RexNode condition = Mockito.mock(RexNode.class);
    RelBuilder finalBuilder = Mockito.mock(RelBuilder.class);
    RelNode expected = Mockito.mock(RelNode.class);

    Mockito.when(builder.field("a")).thenReturn(aField);
    Mockito.when(builder.literal("c")).thenReturn(cValue);

    Mockito.when(builder.call(Mockito.eq(SqlStdOperatorTable.EQUALS), Mockito.eq(aField), Mockito.eq(cValue)))
      .thenReturn(condition);
    Mockito.when(builder.filter(condition)).thenReturn(finalBuilder);
    Mockito.when(finalBuilder.build()).thenReturn(expected);

    QueryContext context = new QueryContext(builder, null);
    RelNode rel = transform.getQuery(context);
    Assert.assertEquals(expected, rel);
  }
}

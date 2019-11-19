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

package io.cdap.pipeline.sql.plugins.projection;

import io.cdap.pipeline.sql.api.template.QueryContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class ProjectionSQLTransformTest {
  @Test(expected = IllegalArgumentException.class)
  public void testEmptySelect() {
    ProjectionSQLTransform.ProjectionSQLTransformConfig config =
      new ProjectionSQLTransform.ProjectionSQLTransformConfig("", "", "");
    config.parseSelect();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptySelectField() {
    ProjectionSQLTransform.ProjectionSQLTransformConfig config =
      new ProjectionSQLTransform.ProjectionSQLTransformConfig("", "", "a,b,,d");
    config.parseSelect();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidRenameKeyPair() {
    ProjectionSQLTransform.ProjectionSQLTransformConfig config =
      new ProjectionSQLTransform.ProjectionSQLTransformConfig("a:,c:d", "", "a,b,,d");
    config.parseRename();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCastKeyPair() {
    ProjectionSQLTransform.ProjectionSQLTransformConfig config =
      new ProjectionSQLTransform.ProjectionSQLTransformConfig("", "a:,b:varchar", "a,b,,d");
    config.parseCast();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameFieldToMultipleNames() {
    ProjectionSQLTransform.ProjectionSQLTransformConfig config =
      new ProjectionSQLTransform.ProjectionSQLTransformConfig("a:d,a:e", "", "a,b,c");
    config.parseRename();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameFieldsToSameName() {
    ProjectionSQLTransform.ProjectionSQLTransformConfig config =
      new ProjectionSQLTransform.ProjectionSQLTransformConfig("a:e,b:e", "", "a,b,c");
    config.parseRename();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCastFieldToMultipleTypes() {
    ProjectionSQLTransform.ProjectionSQLTransformConfig config =
      new ProjectionSQLTransform.ProjectionSQLTransformConfig("", "a:varchar,a:integer", "a,b,c");
    config.parseCast();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCastType() {
    ProjectionSQLTransform.ProjectionSQLTransformConfig config =
      new ProjectionSQLTransform.ProjectionSQLTransformConfig("", "a:x", "a,b,c");
    config.parseCast();
  }

  @Test
  public void testValidProjection() {
    ProjectionSQLTransform.ProjectionSQLTransformConfig config =
      new ProjectionSQLTransform.ProjectionSQLTransformConfig("a:d,b:e", "c:integer", "a,b,c");
    ProjectionSQLTransform transform = new ProjectionSQLTransform(config);

    RelBuilder builder = Mockito.mock(RelBuilder.class);

    RexInputRef a = Mockito.mock(RexInputRef.class);
    RexInputRef b = Mockito.mock(RexInputRef.class);
    RexInputRef c = Mockito.mock(RexInputRef.class);
    RexNode aliasA = Mockito.mock(RexNode.class);
    RexNode aliasB = Mockito.mock(RexNode.class);
    RexNode castedC = Mockito.mock(RexNode.class);
    RelBuilder finalBuilder = Mockito.mock(RelBuilder.class);
    RelNode expected = Mockito.mock(RelNode.class);

    Mockito.when(builder.field("a")).thenReturn(a);
    Mockito.when(builder.field("b")).thenReturn(b);
    Mockito.when(builder.field("c")).thenReturn(c);
    Mockito.when(builder.alias(a, "d")).thenReturn(aliasA);
    Mockito.when(builder.alias(b, "e")).thenReturn(aliasB);
    Mockito.when(builder.cast(c, SqlTypeName.INTEGER)).thenReturn(castedC);
    List<RexNode> fields = new ArrayList<>();
    fields.add(aliasA);
    fields.add(aliasB);
    fields.add(castedC);
    Mockito.when(builder.project(fields)).thenReturn(finalBuilder);
    Mockito.when(finalBuilder.build()).thenReturn(expected);

    QueryContext context = new QueryContext(builder, null);
    RelNode rel = transform.getQuery(context);
    Assert.assertEquals(expected, rel);
  }
}

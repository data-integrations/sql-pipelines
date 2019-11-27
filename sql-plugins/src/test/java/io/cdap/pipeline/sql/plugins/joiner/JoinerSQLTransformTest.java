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

package io.cdap.pipeline.sql.plugins.joiner;

import io.cdap.pipeline.sql.api.template.QueryContext;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class JoinerSQLTransformTest {
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSelectedFields() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig(",x.a as a", "", "INNER");
    config.parseSelectedFields();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSelectedFieldsMissingAlias() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig("x.a,y.b as b", "x.a = y.b", "INNER");
    config.parseSelectedFields();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSelectedFieldsInvalidAlias() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig("x.a as ,y.b as b", "x.a = y.b", "INNER");
    config.parseSelectedFields();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSelectedFieldsSameAlias() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig("x.a as b,y.b as b", "x.a = y.b", "INNER");
    config.parseSelectedFields();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSelectedFieldsSameField() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig("x.a as a,x.a as b", "x.a = y.b", "INNER");
    config.parseSelectedFields();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSelectedFieldsStageNotExist() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig("z.a as a,y.b as b", "x.a = y.b", "INNER");
    Joiner joiner = new Joiner(config);
    List<String> inputs = new ArrayList<>();
    inputs.add("x");
    inputs.add("y");
    RelBuilder builder = Mockito.mock(RelBuilder.class);
    QueryContext context = new QueryContext(builder, inputs);
    joiner.getQuery(context);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidJoinKeyMissingLeftOperand() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig("x.a as a,y.b as b", "= y.b", "INNER");
    config.parseJoinKeys();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidJoinKeyMissingRightOperand() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig("x.a as a,y.b as b", "x.a =", "INNER");
    config.parseJoinKeys();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidJoinKeyMissingStage() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig("x.a as a,y.b as b", ".a = y.b", "INNER");
    config.parseJoinKeys();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidJoinKeyMissingField() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig("x.a as a,y.b as b", "x. = y.b", "INNER");
    config.parseJoinKeys();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidJoinKeyStageNotExist() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig("x.a as a,y.b as b", "z.a = y.b", "INNER");
    Joiner joiner = new Joiner(config);
    List<String> inputs = new ArrayList<>();
    inputs.add("x");
    inputs.add("y");
    RelBuilder builder = Mockito.mock(RelBuilder.class);
    QueryContext context = new QueryContext(builder, inputs);
    joiner.getQuery(context);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnsupportedJoinType() {
    Joiner.JoinerConfig config = new Joiner.JoinerConfig("x.a as a,y.b as b", "x.a = y.b", "invalid");
    config.parseJoinType();
  }
}

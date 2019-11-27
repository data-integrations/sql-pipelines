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

package io.cdap.pipeline.sql.api.template;

import org.apache.calcite.tools.RelBuilder;

import java.util.List;

/**
 * Provides a relational context object which represents the information passed to a plugin during the
 * query generation phase.
 */
public class QueryContext {
  private final RelBuilder relBuilder;
  private final List<String> inputs;

  public QueryContext(RelBuilder relBuilder, List<String> inputs) {
    this.relBuilder = relBuilder;
    this.inputs = inputs;
  }

  /**
   * Gets the {@link RelBuilder} object associated with the current pipeline state.
   * @return The relational expression builder for the plugin to use to build its RelNode
   */
  public RelBuilder getRelBuilder() {
    return relBuilder;
  }

  /**
   * Returns a list of inputs in the order that they are pushed onto the {@link RelBuilder} stack.
   * @return A list of input stage names
   */
  public List<String> getInputs() {
    return inputs;
  }
}

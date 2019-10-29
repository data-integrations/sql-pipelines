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

package io.cdap.pipeline.sql.api.template.interfaces;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;

/**
 * An interface defining an SQL pipeline node. The SQL pipeline node defines transformation behaviors for
 * the inputs passed by the application.
 */
public interface SQLNode {
  /**
   * The application will push all input nodes to a SQLNode to the current {@link RelBuilder} stack.
   *
   * This method should be reserved for logic which needs the overall pipeline context (e.g. joins). For
   * single-input transformations (e.g. aggregation, projection, or filter), use the getQuery() method.
   *
   * @param builder The {@link RelNode} expression builder used to generate the query
   * @param dependencies The number of input nodes which were passed in
   * @return The relational expression builder with one input on the stack
   */
  RelBuilder combineInputs(RelBuilder builder, int dependencies);

  /**
   * Performs a transform on the passed in relational expression builder and returns the relational expression.
   *
   * @param builder The {@link RelNode} expression builder used to generate the query
   * @return The relational expression
   */
  RelNode getQuery(RelBuilder builder);
}

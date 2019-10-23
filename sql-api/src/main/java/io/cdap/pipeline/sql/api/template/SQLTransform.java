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

import io.cdap.pipeline.sql.api.template.interfaces.SQLNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;

/**
 * Represents an abstract SQL transformation node.
 */
public abstract class SQLTransform implements SQLNode {
  public static final String PLUGIN_TYPE = "sqltransform";

  /**
   * By default, the SQLTransform class will perform an implicit union on a node with multiple inputs
   * unless this method is overridden.
   *
   * This method should be reserved for logic which needs the overall pipeline context (e.g. joins). For
   * single-input transformations, use the getQuery() method.
   *
   * @param builder The {@link RelNode} expression builder used to generate the query
   * @param dependencies The number of input nodes which were passed in
   * @return The relational expression builder
   */
  @Override
  public RelBuilder combineInputs(RelBuilder builder, int dependencies) {
    // Perform an implicit union if multiple inputs
    if (dependencies > 1) {
      builder.union(false, dependencies);
    }
    return builder;
  }
}

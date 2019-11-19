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

import io.cdap.pipeline.sql.api.template.QueryContext;
import org.apache.calcite.rel.RelNode;

/**
 * An interface defining an SQL pipeline node. The SQL pipeline node defines transformation behaviors for
 * the inputs passed by the application.
 */
public interface SQLNode {
  /**
   * Performs a transform on the passed in relational expression builder and returns the relational expression.
   *
   * getQuery is guaranteed to be called exactly once for each plugin.
   *
   * @param context The {@link QueryContext} object which contains pipeline info for the plugin to use.
   * @return The relational expression
   */
  RelNode getQuery(QueryContext context);
}

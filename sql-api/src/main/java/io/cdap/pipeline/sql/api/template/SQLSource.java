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

import io.cdap.pipeline.sql.api.template.tables.AbstractTableInfo;

/**
 * Represents an abstract SQL source node.
 */
public abstract class SQLSource extends SQLTransform {
  public static final String PLUGIN_TYPE = "sqlsource";

  /**
   * Returns the table to select from. Must contain the type schema from which the pipeline will query from.
   *
   * @return The table to select from
   */
  public abstract AbstractTableInfo getSourceTable();
}

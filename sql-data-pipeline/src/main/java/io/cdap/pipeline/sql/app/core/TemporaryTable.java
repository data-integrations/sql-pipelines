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

package io.cdap.pipeline.sql.app.core;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * Represents a Calcite temporary table created from a {@link org.apache.calcite.rel.RelNode}'s type class.
 *
 * This table is simply a wrapper around a RelNode and is dynamically added to the type schema of the
 * current {@link org.apache.calcite.tools.RelBuilder} configuration context. These temporary tables are
 * necessary because Calcite performs type and name validation on column and table selection to prevent
 * selecting columns or tables which do not exist. As such, this class allows the application to query
 * from a temporary table without failing validation checks.
 */
public class TemporaryTable extends AbstractTable {
  private final String tableName;
  private final RelDataType type;

  public TemporaryTable(String tableName, RelDataType type) {
    this.tableName = tableName;
    this.type = type;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return type;
  }

  public String getTableName() {
    return tableName;
  }
}

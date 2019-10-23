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

package io.cdap.pipeline.sql.api.template.tables;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;

import javax.annotation.Nullable;

/**
 * Represents a Calcite table delegate with an identifier.
 */
public class DelegateTable extends AbstractTableInfo {
  private final String tableName;
  private final Table table;

  public DelegateTable(String tableName, @Nullable Table table) {
    this.tableName = tableName;
    this.table = table;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (table == null) {
      return null;
    }
    return table.getRowType(typeFactory);
  }

  public String getTableName() {
    return tableName;
  }
}

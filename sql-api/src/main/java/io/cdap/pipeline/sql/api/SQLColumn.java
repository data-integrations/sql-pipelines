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

package io.cdap.pipeline.sql.api;

import io.cdap.pipeline.sql.api.enums.SQLOperandType;
import io.cdap.pipeline.sql.api.interfaces.Aliasable;
import io.cdap.pipeline.sql.api.interfaces.Operand;

import javax.annotation.Nullable;

/**
 * A SQL component which represents a column in a table.
 *
 * A column may be aliased or used as an operand in an expression.
 */
public class SQLColumn implements Aliasable, Operand {
  private final String columnName;
  private final SQLTable columnTable;
  private final String columnAlias;

  public SQLColumn(String name, @Nullable SQLTable table, @Nullable String alias) {
    this.columnName = name;
    this.columnTable = table;
    this.columnAlias = alias;
  }

  @Override
  public boolean hasAlias() {
    return columnAlias != null;
  }

  @Override
  @Nullable
  public String getAlias() {
    return columnAlias;
  }

  @Override
  public SQLOperandType getOperandType() {
    return SQLOperandType.COLUMN;
  }

  public String getName() {
    return columnName;
  }

  public boolean hasTable() {
    return columnTable != null;
  }

  @Nullable
  public SQLTable getTable() {
    return columnTable;
  }
}

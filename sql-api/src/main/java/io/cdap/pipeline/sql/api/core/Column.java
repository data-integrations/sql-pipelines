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

package io.cdap.pipeline.sql.api.core;

import io.cdap.pipeline.sql.api.core.enums.ConstantType;
import io.cdap.pipeline.sql.api.core.enums.OperandType;
import io.cdap.pipeline.sql.api.core.interfaces.Aliasable;
import io.cdap.pipeline.sql.api.core.interfaces.Castable;
import io.cdap.pipeline.sql.api.core.interfaces.Operand;
import io.cdap.pipeline.sql.api.core.interfaces.Queryable;

import javax.annotation.Nullable;

/**
 * A SQL component which represents a column in a table.
 *
 * A column may be aliased or used as an operand in an expression.
 */
public class Column implements Aliasable, Castable, Operand {
  private final String columnName;
  private Queryable columnFrom;
  private final String columnAlias;
  private final ConstantType castType;

  private Column(String name, @Nullable Queryable from, @Nullable String alias, @Nullable ConstantType cast) {
    this.columnName = name;
    this.columnFrom = from;
    this.columnAlias = alias;
    this.castType = cast;
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
  public boolean isCasted() {
    return castType != null;
  }

  @Override
  public ConstantType getCastType() {
    return castType;
  }

  @Override
  public OperandType getOperandType() {
    return OperandType.COLUMN;
  }

  public String getName() {
    return columnName;
  }

  public boolean hasFrom() {
    return columnFrom != null;
  }

  /**
   * The column's source queryable object. Columns should be attached to a queryable which denotes their origin.
   * When creating a query, all columns in the Select field of the {@link StructuredQuery} should originate from
   * the query's corresponding From field.
   *
   * @return The column's origin queryable
   */
  @Nullable
  public Queryable getFrom() {
    return columnFrom;
  }

  public void setFrom(Queryable from) {
    columnFrom = from;
  }

  public static Builder builder(String columnName) {
    return new Builder(columnName);
  }

  /**
   * Builder class for a column component.
   */
  public static class Builder {
    private final String columnName;
    private Queryable columnFrom;
    private String columnAlias;
    private ConstantType castType;

    public Builder(String columnName) {
      this.columnName = columnName;
    }

    public Builder setFrom(Queryable columnFrom) {
      this.columnFrom = columnFrom;
      return this;
    }

    public Builder setAlias(String alias) {
      this.columnAlias = alias;
      return this;
    }

    public Builder setCastType(ConstantType type) {
      this.castType = type;
      return this;
    }

    public Column build() {
      return new Column(columnName, columnFrom, columnAlias, castType);
    }
  }
}

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

import io.cdap.pipeline.sql.api.enums.SQLReadableType;
import io.cdap.pipeline.sql.api.interfaces.Aliasable;
import io.cdap.pipeline.sql.api.interfaces.Filterable;
import io.cdap.pipeline.sql.api.interfaces.SQLReadable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A SQL component which represents an SQL query.
 *
 * A query may be filtered, aliased, or read from as a subquery.
 */
public class SQLQuery implements Aliasable, Filterable, SQLReadable {
  private final List<SQLColumn> selectColumns;
  private final SQLFilter queryFilter;
  private final SQLReadable queryFrom;
  private final String queryAlias;

  private SQLQuery(List<SQLColumn> selectCols, SQLFilter filter, SQLReadable from, @Nullable String alias) {
    selectColumns = Collections.unmodifiableList(selectCols);
    queryFilter = filter;
    queryFrom = from;
    queryAlias = alias;
  }

  public List<SQLColumn> getColumns() {
    return selectColumns;
  }

  @Override
  public boolean hasFilter() {
    return queryFilter != null;
  }

  @Override
  @Nullable
  public SQLFilter getFilter() {
    return queryFilter;
  }

  @Override
  public boolean hasAlias() {
    return queryAlias != null;
  }

  @Override
  @Nullable
  public String getAlias() {
    return queryAlias;
  }

  @Override
  public SQLReadableType getSourceType() {
    return SQLReadableType.QUERY;
  }

  public SQLReadable getQueryFrom() {
    return queryFrom;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(SQLQuery query) {
    return new Builder(query);
  }

  /**
   * A builder class for a {@link SQLQuery}.
   */
  public static class Builder {
    private List<SQLColumn> columns;
    private SQLFilter filter;
    private SQLReadable from;
    private String alias;

    private Builder() {
      columns = new ArrayList<>();
    }

    private Builder(SQLQuery query) {
      this.columns = query.getColumns();
      this.filter = query.getFilter();
      this.from = query.getQueryFrom();
      this.alias = query.getAlias();
    }

    public Builder select(SQLColumn... columns) {
      for (SQLColumn c: columns) {
        this.columns.add(c);
      }
      return this;
    }

    public Builder from(SQLReadable from) {
      this.from = from;
      return this;
    }

    public Builder where(SQLFilter filter) {
      this.filter = filter;
      return this;
    }

    public Builder as(String alias) {
      this.alias = alias;
      return this;
    }

    public SQLQuery build() {
      if (columns == null || columns.size() == 0) {
        throw new IllegalArgumentException("Columns must be provided.");
      } else if (from == null) {
        throw new IllegalArgumentException("A query must read from a readable source.");
      }
      return new SQLQuery(columns, filter, from, alias);
    }
  }
}

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

import io.cdap.pipeline.sql.api.core.enums.QueryableType;
import io.cdap.pipeline.sql.api.core.interfaces.Filterable;
import io.cdap.pipeline.sql.api.core.interfaces.Queryable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A SQL component which represents an SQL query.
 *
 * A query may be filtered, aliased, or read from as a subquery.
 */
public class StructuredQuery implements Filterable, Queryable {
  private final List<Column> selectColumns;
  private final Filter queryFilter;
  private final Queryable queryFrom;
  private final String queryAlias;

  private StructuredQuery(List<Column> selectCols, Filter filter, Queryable from, @Nullable String alias) {
    selectColumns = Collections.unmodifiableList(selectCols);
    queryFilter = filter;
    queryFrom = from;
    queryAlias = alias;
  }

  public List<Column> getColumns() {
    return selectColumns;
  }

  public boolean selectsAllColumns() {
    return selectColumns.size() == 1 && selectColumns.get(0).getName().equals("*");
  }

  @Override
  public boolean hasFilter() {
    return queryFilter != null;
  }

  @Override
  @Nullable
  public Filter getFilter() {
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
  public QueryableType getType() {
    return QueryableType.QUERY;
  }

  @Nullable
  public Queryable getQueryFrom() {
    return queryFrom;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(StructuredQuery query) {
    return new Builder(query);
  }

  /**
   * A builder class for a {@link StructuredQuery}.
   */
  public static class Builder {
    private List<Column> columns;
    private Filter filter;
    private Queryable from;
    private String alias;

    private Builder() {
      columns = new ArrayList<>();
    }

    private Builder(StructuredQuery query) {
      this.columns = new ArrayList<>(query.getColumns());
      this.filter = query.getFilter();
      this.from = query.getQueryFrom();
      this.alias = query.getAlias();
    }

    public List<Column> getColumns() {
      return columns;
    }

    public Builder select(Column... columns) {
      for (Column c: columns) {
        this.columns.add(c);
      }
      return this;
    }

    public Builder selectAllColumns() {
      this.columns.add(Column.builder("*").build());
      return this;
    }

    public Builder from(Queryable from) {
      this.from = from;
      return this;
    }

    public Builder where(Filter filter) {
      this.filter = filter;
      return this;
    }

    public Builder as(String alias) {
      this.alias = alias;
      return this;
    }

    public StructuredQuery build() {
      return new StructuredQuery(columns, filter, from, alias);
    }
  }
}

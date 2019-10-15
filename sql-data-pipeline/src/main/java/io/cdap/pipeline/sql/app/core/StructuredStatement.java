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

import io.cdap.pipeline.sql.api.core.StructuredQuery;
import io.cdap.pipeline.sql.api.core.Table;

import javax.annotation.Nullable;

/**
 * Represents a more generic SQL statement which includes temporary table creation and other similar constructs.
 */
public class StructuredStatement {
  private final StructuredQuery query;
  private final Table temporaryTable;
  private final Table insertIntoTable;

  private StructuredStatement(@Nullable StructuredQuery query,
                              @Nullable Table temporaryTable, @Nullable Table insertIntoTable) {
    this.query = query;
    this.temporaryTable = temporaryTable;
    this.insertIntoTable = insertIntoTable;
  }

  @Nullable
  public StructuredQuery getQuery() {
    return query;
  }

  @Nullable
  public Table getTemporaryTable() {
    return temporaryTable;
  }

  @Nullable
  public Table getInsertIntoTable() {
    return insertIntoTable;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder for the StructuredStatement.
   */
  public static class Builder {
    private StructuredQuery query;
    private Table temporaryTable;
    private Table insertIntoTable;

    public Builder setQuery(StructuredQuery query) {
      this.query = query;
      return this;
    }

    public Builder setTemporaryTable(Table temporaryTable) {
      this.temporaryTable = temporaryTable;
      return this;
    }

    public Builder setInsertIntoTable(Table insertIntoTable) {
      this.insertIntoTable = insertIntoTable;
      return this;
    }

    public StructuredStatement build() {
      return new StructuredStatement(query, temporaryTable, insertIntoTable);
    }
  }
}

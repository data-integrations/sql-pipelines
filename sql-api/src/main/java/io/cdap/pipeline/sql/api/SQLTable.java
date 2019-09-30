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
import io.cdap.pipeline.sql.api.interfaces.SQLReadable;

import javax.annotation.Nullable;

/**
 * A SQL component representing a table in a database.
 *
 * A table is able to be read from and may have an alias.
 */
public class SQLTable implements SQLReadable, Aliasable {
  private final String projectName;
  private final String databaseName;
  private final String tableName;
  private final String tableAlias;

  public SQLTable(@Nullable String project, @Nullable String db, String table, @Nullable String alias) {
    projectName = project;
    databaseName = db;
    tableName = table;
    tableAlias = alias;
  }

  @Override
  public boolean hasAlias() {
    return tableAlias != null;
  }

  @Override
  @Nullable
  public String getAlias() {
    return tableAlias;
  }

  @Override
  public SQLReadableType getSourceType() {
    return SQLReadableType.TABLE;
  }

  public boolean hasProject() {
    return projectName != null;
  }

  @Nullable
  public String getProjectName() {
    return projectName;
  }

  public boolean hasDatabase() {
    return databaseName != null;
  }

  @Nullable
  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }
}

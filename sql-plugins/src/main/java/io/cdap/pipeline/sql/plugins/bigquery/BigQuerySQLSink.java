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

package io.cdap.pipeline.sql.plugins.bigquery;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.pipeline.sql.api.template.QueryContext;
import io.cdap.pipeline.sql.api.template.SQLSink;
import io.cdap.pipeline.sql.api.template.tables.AbstractTableInfo;
import io.cdap.pipeline.sql.api.template.tables.SchemalessTable;
import org.apache.calcite.rel.RelNode;

/**
 * A BigQuery SQL sink.
 */
@Plugin(type = SQLSink.PLUGIN_TYPE)
@Name("BigQueryTable")
@Description("A BigQuery sink.")
public class BigQuerySQLSink extends SQLSink {
  private final BigQuerySQLSinkConfig config;

  @VisibleForTesting
  BigQuerySQLSink(BigQuerySQLSinkConfig config) {
    this.config = config;
  }

  @Override
  public RelNode getQuery(QueryContext context) {
    return context.getRelBuilder().build();
  }

  @Override
  public AbstractTableInfo getDestinationTable() {
    if (Strings.isNullOrEmpty(config.getProject())) {
      throw new IllegalArgumentException("Destination project string must be provided.");
    }
    if (Strings.isNullOrEmpty(config.getDataset())) {
      throw new IllegalArgumentException("Destination dataset string must be provided.");
    }
    if (Strings.isNullOrEmpty(config.getTable())) {
      throw new IllegalArgumentException("Destination table string must be provided.");
    }
    return new SchemalessTable(String.format("%s.%s.%s", config.getProject(), config.getDataset(), config.getTable()));
  }

  /**
   * The configuration class for a BigQuery SQL sink.
   */
  public static class BigQuerySQLSinkConfig extends PluginConfig {
    public static final String PROJECT_NAME = "project";
    public static final String DATASET_NAME = "dataset";
    public static final String TABLE_NAME = "table";
    public static final String SERVICE_ACCOUNT_PATH_NAME = "serviceAccountPath";

    @Name(PROJECT_NAME)
    @Description("The destination project.")
    private final String project;

    @Name(DATASET_NAME)
    @Description("The destination dataset.")
    private final String dataset;

    @Name(TABLE_NAME)
    @Description("The destination table.")
    private final String table;

    @Name(SERVICE_ACCOUNT_PATH_NAME)
    @Description("The path to the service account credentials file.")
    private final String serviceAccountPath;

    public BigQuerySQLSinkConfig(String project, String dataset, String table, String serviceAccountPath) {
      this.project = project;
      this.dataset = dataset;
      this.table = table;
      this.serviceAccountPath = serviceAccountPath;
    }

    public String getProject() {
      return project;
    }

    public String getDataset() {
      return dataset;
    }

    public String getTable() {
      return table;
    }

    public String getServiceAccountPath() {
      return serviceAccountPath;
    }
  }
}

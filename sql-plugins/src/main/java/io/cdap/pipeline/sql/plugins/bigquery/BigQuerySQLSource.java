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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.pipeline.sql.api.template.QueryContext;
import io.cdap.pipeline.sql.api.template.SQLSource;
import io.cdap.pipeline.sql.api.template.tables.AbstractTableInfo;
import io.cdap.pipeline.sql.api.template.tables.DelegateTable;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * A BigQuery SQL source.
 */
@Plugin(type = SQLSource.PLUGIN_TYPE)
@Name("BigQueryTable")
@Description("A BigQuery source.")
public class BigQuerySQLSource extends SQLSource {
  private final BigQuerySQLSourceConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySQLSource.class);

  @VisibleForTesting
  BigQuerySQLSource(BigQuerySQLSourceConfig config) {
    this.config = config;
  }

  @Override
  public RelNode getQuery(QueryContext context) {
    return context.getRelBuilder().build();
  }

  @Override
  public AbstractTableInfo getSourceTable() {
    if (Strings.isNullOrEmpty(config.getProject())) {
      throw new IllegalArgumentException("Destination project string must be provided.");
    }
    if (Strings.isNullOrEmpty(config.getDataset())) {
      throw new IllegalArgumentException("Destination dataset string must be provided.");
    }
    if (Strings.isNullOrEmpty(config.getTable())) {
      throw new IllegalArgumentException("Destination table string must be provided.");
    }
    // Get the schema from BigQuery API
    GoogleCredentials credentials;
    File credentialsPath = new File(config.getServiceAccountPath());
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException("Unable to load service account credentials file.");
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid service account credentials file.");
    }

    // Instantiate a client.
    BigQuery bigquery =
      BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    Table bqTable = bigquery.getTable(TableId.of(config.getProject(), config.getDataset(), config.getTable()));
    Schema bqSchema = bqTable.getDefinition().getSchema();
    BigQueryTable table = new BigQueryTable(bqSchema);
    DelegateTable delegate = new DelegateTable(String.format("%s.%s.%s", config.getProject(),
                                                             config.getDataset(), config.getTable()), table);
    return delegate;
  }

  /**
   * The configuration class for a BigQuery SQL source.
   */
  public static class BigQuerySQLSourceConfig extends PluginConfig {
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

    public BigQuerySQLSourceConfig(String project, String dataset, String table, String serviceAccountPath) {
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

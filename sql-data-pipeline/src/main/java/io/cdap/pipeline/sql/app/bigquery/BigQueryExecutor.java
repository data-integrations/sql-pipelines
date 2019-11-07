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

package io.cdap.pipeline.sql.app.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import io.cdap.pipeline.sql.api.template.SQLTransform;
import io.cdap.pipeline.sql.app.core.AbstractSQLExecutor;
import io.cdap.pipeline.sql.app.core.SQLConfig;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

/**
 * A custom action which executes an SQL pipeline upon BigQuery.
 */
public class BigQueryExecutor extends AbstractSQLExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryExecutor.class);

  private static final String SERVICE_ACCOUNT_PATH_NAME = "serviceAccountPath";

  private String serviceAccountPath;

  public BigQueryExecutor(SQLConfig config, Map<String, SQLTransform> pluginMap) {
    super(config, pluginMap);
  }

  @Override
  public void initialize() {
    super.initialize();

    // Get service account path from runtime arguments
    serviceAccountPath = getContext().getRuntimeArguments().get(SERVICE_ACCOUNT_PATH_NAME);
    if (serviceAccountPath == null) {
      throw new IllegalArgumentException("A service account path must be provided.");
    }
  }

  @Override
  public void run() throws Exception {
    StringBuilder queryBuilder = new StringBuilder();
    // Construct queries
    for (String query: getQueries()) {
      queryBuilder.append(query);
      queryBuilder.append(";\n");
    }

    String queries = queryBuilder.toString();
    LOG.info("Executing queries: " + queries);
    GoogleCredentials credentials;
    File credentialsPath = new File(serviceAccountPath);
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
    }

    // Instantiate a client.
    BigQuery bigquery =
      BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queries).build();
    bigquery.query(queryConfig);
  }

  @Override
  public SqlDialect getDialect() {
    return BigQuerySqlDialect.DEFAULT;
  }
}

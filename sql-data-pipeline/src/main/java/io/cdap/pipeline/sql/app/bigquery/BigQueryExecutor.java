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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.customaction.CustomAction;
import io.cdap.cdap.api.customaction.CustomActionConfigurer;
import io.cdap.cdap.api.customaction.CustomActionContext;
import io.cdap.pipeline.sql.api.core.interfaces.Constant;
import io.cdap.pipeline.sql.api.core.interfaces.Operand;
import io.cdap.pipeline.sql.api.core.interfaces.Queryable;
import io.cdap.pipeline.sql.app.core.DefaultSQLJob;
import io.cdap.pipeline.sql.app.core.StructuredStatement;
import io.cdap.pipeline.sql.app.core.adapters.ConstantAdapter;
import io.cdap.pipeline.sql.app.core.adapters.OperandAdapter;
import io.cdap.pipeline.sql.app.core.adapters.QueryableAdapter;
import io.cdap.pipeline.sql.app.core.interfaces.SQLJob;
import io.cdap.pipeline.sql.serializer.bigquery.BigQuerySQLSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A custom action which executes SQL upon a target platform.
 */
public class BigQueryExecutor implements CustomAction {
  private static final String JOB_NAME = "serializedSqlJob";
  private static final String SERVICE_ACCOUNT_PATH_NAME = "serviceAccountPath";

  private SQLJob job;
  private String serviceAccountPath;

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryExecutor.class);

  public BigQueryExecutor(SQLJob job, String serviceAccountPath) {
    this.job = job;
    this.serviceAccountPath = serviceAccountPath;
  }

  @Override
  public void configure(CustomActionConfigurer configurer) {
    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Queryable.class, new QueryableAdapter())
      .registerTypeAdapter(Operand.class, new OperandAdapter())
      .registerTypeAdapter(Constant.class, new ConstantAdapter())
      .create();
    Map<String, String> properties = new HashMap<>();
    properties.put(JOB_NAME, gson.toJson(job));
    properties.put(SERVICE_ACCOUNT_PATH_NAME, serviceAccountPath);
    configurer.setProperties(properties);
  }

  @Override
  public void initialize(CustomActionContext context) {
    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Queryable.class, new QueryableAdapter())
      .registerTypeAdapter(Operand.class, new OperandAdapter())
      .registerTypeAdapter(Constant.class, new ConstantAdapter())
      .create();
    job = gson.fromJson(context.getSpecification().getProperty(JOB_NAME), DefaultSQLJob.class);
    serviceAccountPath = context.getSpecification().getProperty(SERVICE_ACCOUNT_PATH_NAME);
  }

  @Override
  public void run() throws Exception {
    StringBuilder queryBuilder = new StringBuilder();
    BigQuerySQLSerializer serializer = new BigQuerySQLSerializer();
    // Construct queries
    for (StructuredStatement action: job.getActions()) {
      if (action.getQuery() == null) {
        throw new IllegalArgumentException("Unable to execute statement without a query.");
      }
      if (action.getTemporaryTable() != null) {
        queryBuilder.append(serializer.getSQL(action.getQuery(), action.getTemporaryTable()));
      } else if (action.getInsertIntoTable() != null) {
        queryBuilder.append(serializer.getInsertIntoSQL(action.getQuery(), action.getInsertIntoTable()));
      } else {
        throw new IllegalArgumentException("Statement must contain a table declaration (for now).");
      }
      queryBuilder.append('\n');
    }
    String queries = queryBuilder.toString();
    LOG.info("Executing queries: " + queries);
    GoogleCredentials credentials;
    File credentialsPath = new File(serviceAccountPath);
    FileInputStream serviceAccountStream = new FileInputStream(credentialsPath);
    credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);

    // Instantiate a client.
    BigQuery bigquery =
      BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queries).build();
    bigquery.query(queryConfig);
  }

  @Override
  public void destroy() {}
}

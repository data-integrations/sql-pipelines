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

import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.workflow.AbstractWorkflow;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.pipeline.sql.app.bigquery.BigQueryExecutor;
import io.cdap.pipeline.sql.app.core.interfaces.PluginMapper;

/**
 * Represents the entire workflow in an SQL pipeline.
 */
public class SQLWorkflow extends AbstractWorkflow {
  private final PluginConfigurer pluginConfigurer;
  private final PluginMapper pluginMapper;
  private final SQLConfig config;
  private SQLPipelineSpec spec;

  public SQLWorkflow(SQLConfig config, PluginConfigurer pluginConfigurer) {
    this.config = config;
    this.pluginConfigurer = pluginConfigurer;
    this.pluginMapper = new SQLPluginMapper(pluginConfigurer);
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void configure() {
    SQLPipelineSpecGenerator specGenerator = new SQLPipelineSpecGenerator();
    spec = specGenerator.generateSpec(config, pluginMapper);
    SQLPipelinePlanner planner = new SQLPipelinePlanner();
    addAction(new BigQueryExecutor(planner.plan(spec), config.getServiceAccountPath()));
  }
}

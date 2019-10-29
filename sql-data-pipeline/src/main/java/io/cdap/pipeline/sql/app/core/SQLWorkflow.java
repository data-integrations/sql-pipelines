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
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;
import io.cdap.pipeline.sql.api.template.SQLTransform;
import io.cdap.pipeline.sql.app.bigquery.BigQueryExecutor;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the entire workflow in an SQL pipeline.
 */
public class SQLWorkflow extends AbstractWorkflow {
  private final PluginConfigurer pluginConfigurer;
  private final SQLConfig config;

  public SQLWorkflow(SQLConfig config, PluginConfigurer pluginConfigurer) {
    this.config = config;
    this.pluginConfigurer = pluginConfigurer;
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void configure() {
    Map<String, SQLTransform> pluginMap = new HashMap<>();

    // Map the plugins to plugin identifiers so they may be instantiated at runtime
    for (ETLStage stage: config.getStages()) {
      ETLPlugin plugin = stage.getPlugin();
      TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
        new ArtifactSelectorProvider().getPluginSelector(plugin.getArtifactConfig()));
      Object pluginObject = pluginConfigurer.usePlugin(plugin.getType(), plugin.getName(),
                                                       stage.getName(), plugin.getPluginProperties(), pluginSelector);
      if (pluginObject == null) {
        String errorMessage = String.format("Plugin named '%s' of type '%s' not found.",
                                            plugin.getName(), plugin.getType());
        throw new IllegalStateException(errorMessage);
      }
      pluginMap.put(stage.getName(), (SQLTransform) pluginObject);
    }
    addAction(new BigQueryExecutor(config, pluginMap));
  }
}

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
import io.cdap.pipeline.sql.api.template.SQLSink;
import io.cdap.pipeline.sql.app.bigquery.BigQueryExecutor;
import io.cdap.pipeline.sql.app.core.interfaces.SQLJob;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * Represents the entire workflow in an SQL pipeline.
 */
public class SQLWorkflow extends AbstractWorkflow {
  private final PluginConfigurer pluginConfigurer;
  private final SQLConfig config;
  private SQLPipelineSpec spec;
  private Map<SQLPipelineNode, SQLJob> sinkToJobMap;


  public SQLWorkflow(SQLConfig config, PluginConfigurer pluginConfigurer) {
    this.config = config;
    this.pluginConfigurer = pluginConfigurer;
    this.sinkToJobMap = new LinkedHashMap<>();
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void configure() {
    SQLPipelineSpecGenerator specGenerator = new SQLPipelineSpecGenerator();
    SQLPluginMapper pluginMapper = new SQLPluginMapper(pluginConfigurer);
    spec = specGenerator.generateSpec(config, pluginMapper);

    // DAG traversal and query ordering logic
    // Create query sets for each sink by traversing backwards from the sinks
    // Add dependencies backwards so we don't use temporary tables before creating them
    // This definition set is to prevent nodes from being defined multiple times
    // TODO: Change to hashmap to support different database execution contexts
    Set<SQLPipelineNode> definedNodes = new HashSet<>();
    for (SQLPipelineNode sinkNode: spec.getSinkNodes()) {
      // Create an SQL Job for each sink node
      DefaultSQLJob.Builder jobBuilder = DefaultSQLJob.builder();
      Stack<SQLPipelineNode> queryOrderStack = new Stack<>();
      Stack<SQLPipelineNode> traversalStack = new Stack<>();
      queryOrderStack.add(sinkNode);
      traversalStack.add(sinkNode);
      while (!traversalStack.isEmpty()) {
        SQLPipelineNode currNode = traversalStack.pop();
        for (SQLPipelineNode node: currNode.getFromSet()) {
          if (!definedNodes.contains(node)) {
            queryOrderStack.add(node);
            traversalStack.add(node);
            definedNodes.add(node);
          }
        }
      }
      while (!queryOrderStack.isEmpty()) {
        SQLPipelineNode node = queryOrderStack.pop();
        StructuredStatement.Builder statementBuilder = StructuredStatement.builder();
        statementBuilder.setQuery(node.getQuery());
        if (node.isSink()) {
          statementBuilder.setInsertIntoTable(((SQLSink) node.getPlugin()).getDestinationTable());
        } else {
          statementBuilder.setTemporaryTable(node.getTemporaryTable());
        }
        StructuredStatement statement = statementBuilder.build();
        jobBuilder.addAction(statement);
      }
      sinkToJobMap.put(sinkNode, jobBuilder.build());
    }

    // Combine multiple sink jobs into one job, assume all sinks use same database platform
    DefaultSQLJob.Builder combinedJobBuilder = DefaultSQLJob.builder();
    for (Map.Entry<SQLPipelineNode, SQLJob> pair: sinkToJobMap.entrySet()) {
      combinedJobBuilder.addJob(pair.getValue());
    }
    addAction(new BigQueryExecutor(combinedJobBuilder.build(), config.getServiceAccountPath()));
  }
}

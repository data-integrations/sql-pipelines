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

import io.cdap.pipeline.sql.api.template.SQLSink;
import io.cdap.pipeline.sql.app.core.interfaces.SQLJob;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * Creates the {@link SQLJob} which defines the order of queries to be executed.
 */
public class SQLPipelinePlanner {
  private Map<SQLPipelineNode, SQLJob> sinkToJobMap;

  public SQLPipelinePlanner() {
    sinkToJobMap = new LinkedHashMap<>();
  }

  public SQLJob plan(SQLPipelineSpec spec) {
    // Simple validation
    if (spec == null) {
      throw new IllegalArgumentException("Input specification cannot be null");
    }

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
      Stack<Set<SQLPipelineNode>> visitedStack = new Stack<>();
      queryOrderStack.add(sinkNode);
      traversalStack.add(sinkNode);
      visitedStack.add(new HashSet<>());
      while (!traversalStack.isEmpty()) {
        SQLPipelineNode currNode = traversalStack.pop();
        Set<SQLPipelineNode> visitedSet = visitedStack.pop();
        if (visitedSet.contains(currNode)) {
          throw new IllegalStateException("Detected a loop in the pipeline.");
        } else {
          visitedSet.add(currNode);
        }
        boolean first = true;
        for (SQLPipelineNode node: currNode.getFromSet()) {
          if (!definedNodes.contains(node)) {
            if (first) {
              visitedStack.add(visitedSet);
            } else {
              Set<SQLPipelineNode> visitedClone = new HashSet<>();
              visitedClone.addAll(visitedSet);
              visitedStack.add(visitedClone);
            }
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
    return combinedJobBuilder.build();
  }
}

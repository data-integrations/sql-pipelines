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

import io.cdap.cdap.etl.proto.Connection;
import io.cdap.cdap.etl.proto.v2.ETLConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.pipeline.sql.api.core.Column;
import io.cdap.pipeline.sql.api.core.StructuredQuery;
import io.cdap.pipeline.sql.api.core.Table;
import io.cdap.pipeline.sql.api.template.interfaces.QueryConfigurable;
import io.cdap.pipeline.sql.app.core.interfaces.PluginMapper;

import java.util.HashSet;
import java.util.Set;

/**
 * Generates the spec for an SQL pipeline.
 */
public class SQLPipelineSpecGenerator {
  private static final String TEMPORARY_TABLE_PREFIX = "temporary_table_";

  public SQLPipelineSpec generateSpec(ETLConfig config, PluginMapper mapper) {
    SQLPipelineSpec.Builder specBuilder = SQLPipelineSpec.builder(config);
    // Set the stage node bimap along with temporary table and plugins
    int tempTableCounter = 0;
    // Ensure no stages have the same name
    Set<String> stageNames = new HashSet<>();
    for (ETLStage stage: config.getStages()) {
      // Ensure unique stage names
      if (stageNames.contains(stage.getName())) {
        throw new IllegalArgumentException("Multiple stages share the same name: " + stage.getName());
      } else {
        stageNames.add(stage.getName());
      }

      Table tempTable = new Table(null, null, TEMPORARY_TABLE_PREFIX + tempTableCounter++, null);
      QueryConfigurable plugin = mapper.getMappedPluginInstance(stage.getPlugin(), stage.getName());
      SQLPipelineNode stageNode = new SQLPipelineNode(stage, tempTable, plugin);
      specBuilder.addStageToNodeLink(stage, stageNode);
    }

    // Setup the connections in the linked DAG
    for (Connection conn: config.getConnections()) {
      ETLStage from = null;
      ETLStage to = null;
      for (ETLStage stage : config.getStages()) {
        if (conn.getFrom().equals(stage.getName())) {
          from = stage;
        } else if (conn.getTo().equals(stage.getName())) {
          to = stage;
        }
        if (from != null && to != null) {
          break;
        }
      }
      specBuilder.getSQLNodeFromStage(from).addToNode(specBuilder.getSQLNodeFromStage(to));
      specBuilder.getSQLNodeFromStage(to).addFromNode(specBuilder.getSQLNodeFromStage(from));
    }

    // set sources and sinks and link the nodes together via temporary tables
    for (ETLStage stage: config.getStages()) {
      SQLPipelineNode stageNode = specBuilder.getSQLNodeFromStage(stage);
      if (stageNode.isSource()) {
        specBuilder.addSourceNode(stageNode);
      }
      if (stageNode.isSink()) {
        specBuilder.addSinkNode(stageNode);
      }
      // Assumes every node is a single input node, only link if it is not a source
      // TODO: Add an interface to support combining multiple sources
      if (!stageNode.isSource()) {
        Table prevTable = stageNode.getFromSet().iterator().next().getTemporaryTable();
        StructuredQuery newQuery = updateQuery(stageNode.getPlugin().constructQuery(), prevTable);
        stageNode.setQuery(newQuery);
      } else {
        stageNode.setQuery(stageNode.getPlugin().constructQuery());
      }
    }
    return specBuilder.build();
  }

  private StructuredQuery updateQuery(StructuredQuery query, Table prevTable) {
    StructuredQuery.Builder builder = StructuredQuery.builder(query);
    if (query.getQueryFrom() == null) {
      builder.from(prevTable);
      if (builder.getColumns().isEmpty()) {
        builder.selectAllColumns();
      } else {
        for (Column col : builder.getColumns()) {
          col.setFrom(prevTable);
        }
      }
    }
    return builder.build();
  }
}

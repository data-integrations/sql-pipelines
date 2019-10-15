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

import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.pipeline.sql.api.core.StructuredQuery;
import io.cdap.pipeline.sql.api.core.Table;
import io.cdap.pipeline.sql.api.template.interfaces.QueryConfigurable;

import java.util.HashSet;
import java.util.Set;

/**
 * Represents a node with a corresponding ETL Stage in the pipeline representation of a directed acyclic graph.
 */
public class SQLPipelineNode {
  private final ETLStage stage;
  private final Set<SQLPipelineNode> fromSet;
  private final Set<SQLPipelineNode> toSet;
  private final Table temporaryTable;
  private final QueryConfigurable plugin;
  private StructuredQuery query;

  public SQLPipelineNode(ETLStage stage, Table temporaryTable, QueryConfigurable plugin) {
    this.stage = stage;
    this.temporaryTable = temporaryTable;
    this.plugin = plugin;
    this.fromSet = new HashSet<>();
    this.toSet = new HashSet<>();
  }

  public ETLStage getStage() {
    return stage;
  }

  public void addFromNode(SQLPipelineNode node) {
    fromSet.add(node);
  }

  public Set<SQLPipelineNode> getFromSet() {
    return fromSet;
  }

  public boolean isSource() {
    return fromSet.isEmpty();
  }

  public void addToNode(SQLPipelineNode node) {
    toSet.add(node);
  }

  public Set<SQLPipelineNode> getToSet() {
    return toSet;
  }

  public boolean isSink() {
    return toSet.isEmpty();
  }

  public Table getTemporaryTable() {
    return temporaryTable;
  }

  public void setQuery(StructuredQuery query) {
    this.query = query;
  }

  public StructuredQuery getQuery() {
    return query;
  }

  public QueryConfigurable getPlugin() {
    return plugin;
  }
}

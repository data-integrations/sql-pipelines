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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import io.cdap.cdap.etl.proto.v2.ETLConfig;
import io.cdap.cdap.etl.proto.v2.ETLStage;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents the pipeline spec for an SQL pipeline.
 */
public class SQLPipelineSpec {
  private final ETLConfig config;
  private final Set<SQLPipelineNode> sourceNodes;
  private final Set<SQLPipelineNode> sinkNodes;
  private final BiMap<ETLStage, SQLPipelineNode> stageNodes;

  private SQLPipelineSpec(ETLConfig config, Set<SQLPipelineNode> sourceNodes, Set<SQLPipelineNode> sinkNodes,
                          BiMap<ETLStage, SQLPipelineNode> stageNodes) {
    this.config = config;
    this.sourceNodes = Collections.unmodifiableSet(sourceNodes);
    this.sinkNodes = Collections.unmodifiableSet(sinkNodes);
    this.stageNodes = ImmutableBiMap.copyOf(stageNodes);
  }

  public ETLConfig getConfig() {
    return config;
  }

  public Set<SQLPipelineNode> getSourceNodes() {
    return sourceNodes;
  }

  public Set<SQLPipelineNode> getSinkNodes() {
    return sinkNodes;
  }

  public BiMap<ETLStage, SQLPipelineNode> getStageNodesBiMap() {
    return stageNodes;
  }

  public static Builder builder(ETLConfig config) {
    return new Builder(config);
  }

  /**
   * Builder for creating a SQLPipelineSpec.
   */
  public static class Builder {
    private final ETLConfig config;
    private final Set<SQLPipelineNode> sourceNodes;
    private final Set<SQLPipelineNode> sinkNodes;
    private final BiMap<ETLStage, SQLPipelineNode> stageNodes;

    public Builder(ETLConfig config) {
      this.config = config;
      this.sourceNodes = new HashSet<>();
      this.sinkNodes = new HashSet<>();
      this.stageNodes = HashBiMap.create();
    }

    public Builder addSourceNode(SQLPipelineNode node) {
      sourceNodes.add(node);
      return this;
    }

    public Builder addSinkNode(SQLPipelineNode node) {
      sinkNodes.add(node);
      return this;
    }

    public Builder addStageToNodeLink(ETLStage stage, SQLPipelineNode node) {
      stageNodes.put(stage, node);
      return this;
    }

    public SQLPipelineNode getSQLNodeFromStage(ETLStage stage) {
      return stageNodes.get(stage);
    }

    public SQLPipelineSpec build() {
      return new SQLPipelineSpec(config, sourceNodes, sinkNodes, stageNodes);
    }
  }
}

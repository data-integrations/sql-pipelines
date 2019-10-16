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

package io.cdap.pipeline.sql.app;

import io.cdap.cdap.etl.proto.Connection;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.pipeline.sql.api.core.StructuredQuery;
import io.cdap.pipeline.sql.api.template.interfaces.QueryConfigurable;
import io.cdap.pipeline.sql.app.core.SQLConfig;
import io.cdap.pipeline.sql.app.core.SQLPipelineNode;
import io.cdap.pipeline.sql.app.core.SQLPipelineSpec;
import io.cdap.pipeline.sql.app.core.SQLPipelineSpecGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SQLPipelineSpecGeneratorTest {
  @Test
  public void testSingleNode() {
    // node
    ETLStage testStage = new ETLStage("test", null);
    // config
    Set<ETLStage> stages = new HashSet<>();
    stages.add(testStage);
    Set<Connection> connections = new HashSet<>();
    SQLConfig config = new SQLConfig(null, stages, connections, new HashMap<>());
    // query
    StructuredQuery query = StructuredQuery.builder().build();
    MockPlugin mockPlugin = new MockPlugin(query, null);
    Map<String, QueryConfigurable> pluginMap = new HashMap<>();
    pluginMap.put("test", mockPlugin);

    SQLPipelineSpecGenerator generator = new SQLPipelineSpecGenerator();
    SQLPipelineSpec spec = generator.generateSpec(config, new MockPluginMapper(pluginMap));

    // general config
    Assert.assertEquals(1, spec.getSourceNodes().size());
    Assert.assertEquals(testStage, spec.getSourceNodes().iterator().next().getStage());
    Assert.assertEquals(1, spec.getSinkNodes().size());
    Assert.assertEquals(testStage, spec.getSinkNodes().iterator().next().getStage());

    // stage specific
    SQLPipelineNode node = spec.getStageNodesBiMap().get(testStage);
    Assert.assertEquals(testStage, node.getStage());
    Assert.assertEquals(mockPlugin, node.getPlugin());
    Assert.assertEquals(1, mockPlugin.getConstructCount());
    Assert.assertEquals(0, node.getFromSet().size());
    Assert.assertEquals(0, node.getToSet().size());
    Assert.assertEquals(query, node.getQuery());
  }

  @Test
  public void testTwoNodes() {
    final String firstName = "test1";
    final String secondName = "test2";
    // node 1
    ETLStage stage1 = new ETLStage(firstName, null);
    // Node 2
    ETLStage stage2 = new ETLStage(secondName, null);
    // config
    Set<ETLStage> stages = new HashSet<>();
    stages.add(stage1);
    stages.add(stage2);
    Set<Connection> connections = new HashSet<>();
    Connection conn = new Connection(stage1.getName(), stage2.getName());
    connections.add(conn);
    SQLConfig config = new SQLConfig(null, stages, connections, new HashMap<>());
    // query
    StructuredQuery query1 = StructuredQuery.builder().build();
    StructuredQuery query2 = StructuredQuery.builder().build();
    MockPlugin mockPlugin1 = new MockPlugin(query1, null);
    MockPlugin mockPlugin2 = new MockPlugin(query2, null);
    Map<String, QueryConfigurable> pluginMap = new HashMap<>();
    pluginMap.put(firstName, mockPlugin1);
    pluginMap.put(secondName, mockPlugin2);

    SQLPipelineSpecGenerator generator = new SQLPipelineSpecGenerator();
    SQLPipelineSpec spec = generator.generateSpec(config, new MockPluginMapper(pluginMap));

    // general config
    Assert.assertEquals(1, spec.getSourceNodes().size());
    Assert.assertEquals(stage1, spec.getSourceNodes().iterator().next().getStage());
    Assert.assertEquals(1, spec.getSinkNodes().size());
    Assert.assertEquals(stage2, spec.getSinkNodes().iterator().next().getStage());

    // stage 1 specific
    SQLPipelineNode node1 = spec.getStageNodesBiMap().get(stage1);
    Assert.assertEquals(stage1, node1.getStage());
    Assert.assertEquals(mockPlugin1, node1.getPlugin());
    Assert.assertEquals(1, mockPlugin1.getConstructCount());
    Assert.assertEquals(0, node1.getFromSet().size());
    Assert.assertEquals(1, node1.getToSet().size());
    Assert.assertEquals(query1, node1.getQuery());

    // stage 2 specific
    SQLPipelineNode node2 = spec.getStageNodesBiMap().get(stage2);
    Assert.assertEquals(stage2, node2.getStage());
    Assert.assertEquals(mockPlugin2, node2.getPlugin());
    Assert.assertEquals(1, mockPlugin2.getConstructCount());
    Assert.assertEquals(1, node2.getFromSet().size());
    Assert.assertEquals(0, node2.getToSet().size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSameNameNodes() {
    final String firstName = "test1";
    final String secondName = "test1";
    ETLPlugin plugin1 = new ETLPlugin("plugin1", null, new HashMap<>());
    ETLPlugin plugin2 = new ETLPlugin("plugin2", null, new HashMap<>());
    // node 1
    ETLStage stage1 = new ETLStage(firstName, plugin1);
    // Node 2
    ETLStage stage2 = new ETLStage(secondName, plugin2);
    // config
    Set<ETLStage> stages = new HashSet<>();
    stages.add(stage1);
    stages.add(stage2);
    Set<Connection> connections = new HashSet<>();
    Connection conn = new Connection(stage1.getName(), stage2.getName());
    connections.add(conn);
    SQLConfig config = new SQLConfig(null, stages, connections, new HashMap<>());
    // query
    StructuredQuery query1 = StructuredQuery.builder().build();
    StructuredQuery query2 = StructuredQuery.builder().build();
    MockPlugin mockPlugin1 = new MockPlugin(query1, null);
    MockPlugin mockPlugin2 = new MockPlugin(query2, null);
    Map<String, QueryConfigurable> pluginMap = new HashMap<>();
    pluginMap.put(firstName, mockPlugin1);
    pluginMap.put(secondName, mockPlugin2);

    SQLPipelineSpecGenerator generator = new SQLPipelineSpecGenerator();
    generator.generateSpec(config, new MockPluginMapper(pluginMap));
  }
}

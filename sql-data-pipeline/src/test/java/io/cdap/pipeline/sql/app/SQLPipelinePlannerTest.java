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

import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.pipeline.sql.api.core.StructuredQuery;
import io.cdap.pipeline.sql.api.core.Table;
import io.cdap.pipeline.sql.app.core.SQLPipelineNode;
import io.cdap.pipeline.sql.app.core.SQLPipelinePlanner;
import io.cdap.pipeline.sql.app.core.SQLPipelineSpec;
import io.cdap.pipeline.sql.app.core.interfaces.SQLJob;
import org.junit.Assert;
import org.junit.Test;

public class SQLPipelinePlannerTest {
  @Test(expected = IllegalArgumentException.class)
  public void testNullInputSpec() {
    SQLPipelinePlanner planner = new SQLPipelinePlanner();
    planner.plan(null);
  }

  @Test
  public void testSingleNode() {
    // node
    ETLStage testStage = new ETLStage("test", null);
    StructuredQuery query = StructuredQuery.builder().build();
    Table destinationTable = new Table("a", "b", "c", null);
    MockPlugin mockPlugin = new MockPlugin(query, destinationTable);
    SQLPipelineNode node = new SQLPipelineNode(testStage, null, mockPlugin);
    node.setQuery(query);
    // spec
    SQLPipelineSpec spec = SQLPipelineSpec.builder(null)
      .addSourceNode(node)
      .addSinkNode(node)
      .addStageToNodeLink(testStage, node)
      .build();

    SQLPipelinePlanner planner = new SQLPipelinePlanner();
    SQLJob job = planner.plan(spec);
    Assert.assertEquals(1, mockPlugin.getDestinationTableCount());
    Assert.assertEquals(1, job.getActions().size());
    Assert.assertEquals(query, job.getActions().get(0).getQuery());
    Assert.assertEquals(destinationTable, job.getActions().get(0).getInsertIntoTable());
  }

  @Test(expected = IllegalStateException.class)
  public void testSingleNodeCycle() {
    // node
    ETLStage testStage = new ETLStage("test", null);
    StructuredQuery query = StructuredQuery.builder().build();
    Table destinationTable = new Table("a", "b", "c", null);
    MockPlugin mockPlugin = new MockPlugin(query, destinationTable);
    SQLPipelineNode node = new SQLPipelineNode(testStage, null, mockPlugin);
    node.setQuery(query);
    node.addToNode(node);
    node.addFromNode(node);
    // spec
    SQLPipelineSpec spec = SQLPipelineSpec.builder(null)
      .addSourceNode(node)
      .addSinkNode(node)
      .addStageToNodeLink(testStage, node)
      .build();

    SQLPipelinePlanner planner = new SQLPipelinePlanner();
    SQLJob job = planner.plan(spec);
    Assert.assertEquals(1, mockPlugin.getDestinationTableCount());
    Assert.assertEquals(1, job.getActions().size());
    Assert.assertEquals(query, job.getActions().get(0).getQuery());
    Assert.assertEquals(destinationTable, job.getActions().get(0).getInsertIntoTable());
  }

  @Test
  public void testTwoNodes() {
    // source
    ETLStage testStage1 = new ETLStage("test1", null);
    StructuredQuery query1 = StructuredQuery.builder().build();
    MockPlugin mockPlugin1 = new MockPlugin(query1, null);
    SQLPipelineNode node1 = new SQLPipelineNode(testStage1, null, mockPlugin1);
    node1.setQuery(query1);
    // sink
    ETLStage testStage2 = new ETLStage("test2", null);
    StructuredQuery query2 = StructuredQuery.builder().build();
    Table destinationTable = new Table("a", "b", "c", null);
    MockPlugin mockPlugin2 = new MockPlugin(query2, destinationTable);
    SQLPipelineNode node2 = new SQLPipelineNode(testStage2, null, mockPlugin2);
    node2.setQuery(query2);
    // connections
    node1.addToNode(node2);
    node2.addFromNode(node1);
    // spec
    SQLPipelineSpec spec = SQLPipelineSpec.builder(null)
      .addSourceNode(node1)
      .addSinkNode(node2)
      .addStageToNodeLink(testStage1, node1)
      .addStageToNodeLink(testStage2, node2)
      .build();

    SQLPipelinePlanner planner = new SQLPipelinePlanner();
    SQLJob job = planner.plan(spec);
    Assert.assertEquals(0, mockPlugin1.getDestinationTableCount());
    Assert.assertEquals(1, mockPlugin2.getDestinationTableCount());
    Assert.assertEquals(2, job.getActions().size());
    Assert.assertEquals(query1, job.getActions().get(0).getQuery());
    Assert.assertEquals(query2, job.getActions().get(1).getQuery());
    Assert.assertEquals(destinationTable, job.getActions().get(1).getInsertIntoTable());
  }

  @Test
  public void testThreeLinearNodes() {
    // source
    ETLStage testStage1 = new ETLStage("test1", null);
    StructuredQuery query1 = StructuredQuery.builder().build();
    MockPlugin mockPlugin1 = new MockPlugin(query1, null);
    SQLPipelineNode node1 = new SQLPipelineNode(testStage1, null, mockPlugin1);
    node1.setQuery(query1);
    // transform
    ETLStage testStage2 = new ETLStage("test2", null);
    StructuredQuery query2 = StructuredQuery.builder().build();
    MockPlugin mockPlugin2 = new MockPlugin(query2, null);
    SQLPipelineNode node2 = new SQLPipelineNode(testStage2, null, mockPlugin2);
    node2.setQuery(query2);
    // sink
    ETLStage testStage3 = new ETLStage("test2", null);
    StructuredQuery query3 = StructuredQuery.builder().build();
    Table destinationTable = new Table("a", "b", "c", null);
    MockPlugin mockPlugin3 = new MockPlugin(query3, destinationTable);
    SQLPipelineNode node3 = new SQLPipelineNode(testStage2, null, mockPlugin3);
    node3.setQuery(query3);
    // connections
    node1.addToNode(node2);
    node2.addFromNode(node1);
    node2.addToNode(node3);
    node3.addFromNode(node2);
    // spec
    SQLPipelineSpec spec = SQLPipelineSpec.builder(null)
      .addSourceNode(node1)
      .addSinkNode(node3)
      .addStageToNodeLink(testStage1, node1)
      .addStageToNodeLink(testStage2, node2)
      .addStageToNodeLink(testStage3, node3)
      .build();

    SQLPipelinePlanner planner = new SQLPipelinePlanner();
    SQLJob job = planner.plan(spec);
    Assert.assertEquals(0, mockPlugin1.getDestinationTableCount());
    Assert.assertEquals(0, mockPlugin2.getDestinationTableCount());
    Assert.assertEquals(1, mockPlugin3.getDestinationTableCount());
    Assert.assertEquals(3, job.getActions().size());
    Assert.assertEquals(query1, job.getActions().get(0).getQuery());
    Assert.assertEquals(query2, job.getActions().get(1).getQuery());
    Assert.assertEquals(query3, job.getActions().get(2).getQuery());
    Assert.assertEquals(destinationTable, job.getActions().get(2).getInsertIntoTable());
  }

  /**
   * This function is useful for creating a mock {@link SQLPipelineNode} when you don't need
   * to validate its individual components.
   * @param stage The {@link ETLStage} associated with the node
   * @return A new node
   */
  private SQLPipelineNode createMockNode(ETLStage stage) {
    StructuredQuery query = StructuredQuery.builder().build();
    MockPlugin mockPlugin = new MockPlugin(query, null);
    SQLPipelineNode node = new SQLPipelineNode(stage, null, mockPlugin);
    node.setQuery(query);
    return node;
  }

  /**
   * This test checks that a cycle is detected in the following graph:
   *
   *                          B -->-- D
   *                        /          \
   *                   A ->-            ->- F
   *                   \    \          /
   *                    \    C -->-- E
   *                     \           \
   *                      \          /
   *                       ----<----
   *
   *               Connection E -> A is a back edge.
   */
  @Test(expected = IllegalStateException.class)
  public void testMultiNodeNonCycle() {
    // A
    ETLStage stageA = new ETLStage("A", null);
    SQLPipelineNode nodeA = createMockNode(stageA);
    // B
    ETLStage stageB = new ETLStage("B", null);
    SQLPipelineNode nodeB = createMockNode(stageB);
    // C
    ETLStage stageC = new ETLStage("C", null);
    SQLPipelineNode nodeC = createMockNode(stageC);
    //D
    ETLStage stageD = new ETLStage("D", null);
    SQLPipelineNode nodeD = createMockNode(stageD);
    // E
    ETLStage stageE = new ETLStage("E", null);
    SQLPipelineNode nodeE = createMockNode(stageE);
    // F
    ETLStage stageF = new ETLStage("F", null);
    SQLPipelineNode nodeF = createMockNode(stageF);
    // connections
    nodeA.addFromNode(nodeE);
    nodeA.addToNode(nodeB);
    nodeA.addToNode(nodeC);
    nodeB.addFromNode(nodeA);
    nodeB.addToNode(nodeD);
    nodeC.addFromNode(nodeA);
    nodeC.addToNode(nodeE);
    nodeD.addFromNode(nodeB);
    nodeD.addToNode(nodeF);
    nodeE.addFromNode(nodeC);
    nodeE.addToNode(nodeF);
    nodeE.addToNode(nodeA);
    nodeF.addFromNode(nodeD);
    nodeF.addFromNode(nodeE);
    // spec
    SQLPipelineSpec spec = SQLPipelineSpec.builder(null)
      .addSourceNode(nodeA)
      .addSinkNode(nodeF)
      .addStageToNodeLink(stageA, nodeA)
      .addStageToNodeLink(stageB, nodeB)
      .addStageToNodeLink(stageC, nodeC)
      .addStageToNodeLink(stageD, nodeD)
      .addStageToNodeLink(stageE, nodeE)
      .addStageToNodeLink(stageF, nodeF)
      .build();
    SQLPipelinePlanner planner = new SQLPipelinePlanner();
    planner.plan(spec);
  }
}

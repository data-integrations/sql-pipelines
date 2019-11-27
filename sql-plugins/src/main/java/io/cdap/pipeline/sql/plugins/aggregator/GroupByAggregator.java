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

package io.cdap.pipeline.sql.plugins.aggregator;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.pipeline.sql.api.template.QueryContext;
import io.cdap.pipeline.sql.api.template.SQLTransform;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL group by aggregator.
 */
@Plugin(type = SQLTransform.PLUGIN_TYPE)
@Name("GroupByAggregate")
@Description("Groups by one or more fields, then performs one or more aggregate functions on each group. " +
  "Supports avg, count, count(*), max, min, and sum as aggregate functions.")
public class GroupByAggregator extends SQLTransform {
  private final GroupByConfig conf;
  private Map<String, GroupByConfig.AggregateInfo> aggregateFunctions;
  private List<String> groupByFields;

  @VisibleForTesting
  GroupByAggregator(GroupByConfig conf) {
    this.conf = conf;
  }

  @Override
  public RelNode getQuery(QueryContext context) {
    RelBuilder builder = context.getRelBuilder();

    // Initialize
    init();

    List<RexNode> groupFields = new ArrayList<>();
    List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
    for (String fieldName: groupByFields) {
      groupFields.add(builder.field(fieldName));
    }
    for (Map.Entry<String, GroupByConfig.AggregateInfo> entry: aggregateFunctions.entrySet()) {
      GroupByConfig.AggregateInfo aggInfo = entry.getValue();
      RelBuilder.AggCall agg = getAggCall(builder, aggInfo.getAlias(), aggInfo.getAggregateFunction(),
                                          aggInfo.getField());
      aggCalls.add(agg);
    }
    RelBuilder.GroupKey groupKey = null;
    if (groupFields.size() != 0) {
      groupKey = builder.groupKey(groupFields);
    }
    return builder.aggregate(groupKey, aggCalls).build();
  }

  private void init() {
    groupByFields = conf.getGroupByFields();
    List<GroupByConfig.AggregateInfo> aggregateInfos = conf.getAggregates();
    aggregateFunctions = new LinkedHashMap<>();
    for (GroupByConfig.AggregateInfo aggInfo: aggregateInfos) {
      aggregateFunctions.put(aggInfo.getField(), aggInfo);
    }
  }

  private RelBuilder.AggCall getAggCall(RelBuilder builder, String alias, String functionStr, String... fields) {
    switch (functionStr.toUpperCase()) {
      case "AVG":
        return builder.avg(builder.field(fields[0])).as(alias);
      case "COUNT":
        if ("*".equals(fields[0])) {
          return builder.countStar(alias);
        } else {
          List<RexNode> operands = new ArrayList<>();
          for (String field : fields) {
            operands.add(builder.field(field));
          }
          return builder.count(operands).as(alias);
        }
      case "MIN":
        return builder.min(builder.field(fields[0])).as(alias);
      case "MAX":
        return builder.max(builder.field(fields[0])).as(alias);
      case "SUM":
        return builder.sum(builder.field(fields[0])).as(alias);
      default:
        throw new IllegalArgumentException("Unsupported aggregate function " + functionStr);
    }
  }
}

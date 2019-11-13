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
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Config for group by types of plugins.
 */
public class GroupByConfig extends PluginConfig {
  private static final String AGGREGATES_NAME = "aggregates";
  private static final String GROUP_BY_FIELDS_NAME = "groupByFields";
  private static final String AGGREGATES_DESC = "Aggregates to compute on grouped records. " +
    "Supported aggregate functions are count, count(*), sum, avg, min, max. " +
    "A function must specify the field it should be applied on, as well as the name it should be called. " +
    "Aggregates are specified using syntax: \"name:function(field)[, other aggregates]\"." +
    "For example, 'avgPrice:avg(price),cheapest:min(price)' will calculate two aggregates. " +
    "The first will create a field called 'avgPrice' that is the average of all 'price' fields in the group. " +
    "The second will create a field called 'cheapest' that contains the minimum 'price' field in the group.";
  private static final String GROUP_BY_FIELDS_DESC = "Comma separated list of record fields to group by. " +
    "All records with the same value for all these fields will be grouped together. " +
    "The records output by this aggregator will contain all the group by fields and aggregate fields. " +
    "For example, if grouping by the 'user' field and calculating a count aggregate called 'numActions', " +
    "output records will have a 'user' field and 'numActions' field.";

  @Name(AGGREGATES_NAME)
  @Description(AGGREGATES_DESC)
  private final String aggregates;

  @Name(GROUP_BY_FIELDS_NAME)
  @Description(GROUP_BY_FIELDS_DESC)
  private final String groupByFields;

  @VisibleForTesting
  GroupByConfig(String groupByFields, String aggregates) {
    this.groupByFields = groupByFields;
    this.aggregates = aggregates;
  }

  /**
   * @return the fields to group by. Returns an empty list if groupByFields contains a macro. Otherwise, the list
   *         returned can never be empty.
   */
  List<String> getGroupByFields() {
    List<String> fields = new ArrayList<>();
    if (Strings.isNullOrEmpty(groupByFields)) {
      return fields;
    }
    for (String field : Splitter.on(',').trimResults().split(groupByFields)) {
      if (Strings.isNullOrEmpty(field)) {
        throw new IllegalArgumentException("Group By field may not be empty.");
      }
      fields.add(field);
    }
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("The 'groupByFields' property must be set.");
    }
    return fields;
  }

  /**
   * @return A map of alias name to aggregate function to perform.
   */
  List<AggregateInfo> getAggregates() {
    List<AggregateInfo> aggregatesList = new ArrayList<>();
    if (Strings.isNullOrEmpty(aggregates)) {
      return aggregatesList;
    }
    Set<String> names = new HashSet<>();
    for (String aggregate : Splitter.on(',').trimResults().split(aggregates)) {
      int colonIdx = aggregate.indexOf(':');
      if (colonIdx < 0) {
        throw new IllegalArgumentException(String.format(
          "Could not find ':' separating aggregate name from its function in '%s'.", aggregate));
      }
      String name = aggregate.substring(0, colonIdx).trim();
      if (Strings.isNullOrEmpty(name)) {
        throw new IllegalArgumentException("Aggregate alias must not be empty.");
      }
      if (names.contains(name)) {
        throw new IllegalArgumentException(String.format(
          "Cannot create multiple aggregate functions with the same name '%s'.", name));
      }

      String functionAndField = aggregate.substring(colonIdx + 1).trim();
      int leftParamIdx = functionAndField.indexOf('(');
      if (leftParamIdx < 0) {
        throw new IllegalArgumentException(String.format(
          "Could not find '(' in function '%s'. Functions must be specified as function(field).",
          functionAndField));
      }
      String functionStr = functionAndField.substring(0, leftParamIdx).trim();
      if (!functionAndField.endsWith(")")) {
        throw new IllegalArgumentException(String.format(
          "Could not find closing ')' in function '%s'. Functions must be specified as function(field).",
          functionAndField));
      }
      String field = functionAndField.substring(leftParamIdx + 1, functionAndField.length() - 1).trim();
      if (field.isEmpty()) {
        throw new IllegalArgumentException(String.format(
          "Invalid function '%s'. A field must be given as an argument.", functionAndField));
      }
      aggregatesList.add(new AggregateInfo(name, functionStr, field));
    }

    if (aggregatesList.isEmpty()) {
      throw new IllegalArgumentException("The 'aggregates' property must be set.");
    }
    return aggregatesList;
  }

  /**
   * Represents a field aggregation.
   */
  public class AggregateInfo {
    private final String alias;
    private final String aggregateFunction;
    private final String field;

    public AggregateInfo(String alias, String aggregateFunction, String field) {
      this.alias = alias;
      this.aggregateFunction = aggregateFunction;
      this.field = field;
    }

    public String getAlias() {
      return alias;
    }

    public String getAggregateFunction() {
      return aggregateFunction;
    }

    public String getField() {
      return field;
    }
  }
}

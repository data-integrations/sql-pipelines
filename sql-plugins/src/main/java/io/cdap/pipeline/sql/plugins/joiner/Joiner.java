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

package io.cdap.pipeline.sql.plugins.joiner;


import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.pipeline.sql.api.template.QueryContext;
import io.cdap.pipeline.sql.api.template.SQLJoiner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Represents an SQL Join operation.
 */
@Plugin(type = SQLJoiner.PLUGIN_TYPE)
@Name("Joiner")
@Description("Represents a simple SQL join operation on two inputs.")
public class Joiner extends SQLJoiner {
  private static final Logger LOG = LoggerFactory.getLogger(Joiner.class);
  private final JoinerConfig config;
  private JoinRelType calciteJoinType;
  private Map<String, String> selectedFields;
  private List<JoinerConfig.JoinKey> joinKeys;
  private Map<String, Integer> stageOrdinalMap;

  public Joiner(JoinerConfig config) {
    this.config = config;
  }

  @Override
  public RelNode getQuery(QueryContext context) {
    RelBuilder builder = context.getRelBuilder();
    List<String> inputs = context.getInputs();
    if (inputs.size() != 2) {
      throw new IllegalArgumentException("Unable to join more than two inputs. Please use multiple joins for more " +
                                           "than two sources.");
    }
    // Initialize config
    init(inputs);

    // Construct the join
    List<RexNode> conditions = new ArrayList<>();
    for (JoinerConfig.JoinKey key: joinKeys) {
      if (!stageOrdinalMap.containsKey(key.getLeftStage()) || !stageOrdinalMap.containsKey(key.getRightStage())) {
        throw new IllegalArgumentException("Join key stage is not in the list of input stages.");
      }
      int leftStageOrdinal = stageOrdinalMap.get(key.getLeftStage());
      RexNode leftField = builder.field(inputs.size(), leftStageOrdinal, key.getLeftKey());
      int rightStageOrdinal = stageOrdinalMap.get(key.getRightStage());
      RexNode rightField = builder.field(inputs.size(), rightStageOrdinal, key.getRightKey());
      RexNode condition = builder.call(SqlStdOperatorTable.EQUALS, leftField, rightField);
      conditions.add(condition);
    }
    builder.join(calciteJoinType, conditions);

    // Construct the projection
    List<RexNode> fields = new ArrayList<>();
    for (Map.Entry<String, String> selectedFieldsEntry: selectedFields.entrySet()) {
      String fullFieldName = selectedFieldsEntry.getKey();
      String[] nameArr = fullFieldName.split(Pattern.quote("."));
      String stageName = nameArr[0];
      String fieldName = nameArr[1];
      if (!stageOrdinalMap.containsKey(stageName)) {
        throw new IllegalArgumentException("Selected field stage is not in the list of input stages.");
      }
      String alias = selectedFieldsEntry.getValue();
      RexNode field = builder.alias(builder.field(fieldName), alias);
      fields.add(field);
    }
    return builder.project(fields).build();
  }

  private void init(List<String> inputs) {
    // Parse the config strings
    selectedFields = config.parseSelectedFields();
    joinKeys = config.parseJoinKeys();

    // Get a map of the ordinals
    stageOrdinalMap = new HashMap<>();
    for (int i = 0; i < inputs.size(); i++) {
      stageOrdinalMap.put(inputs.get(i), i);
    }

    // Parse the join type
    calciteJoinType = config.parseJoinType();
  }

  /**
   * The configuration class for a simple joiner.
   */
  public static class JoinerConfig extends PluginConfig {
    private static final String SELECTED_FIELDS_NAME = "selectedFields";
    private static final String JOIN_KEY_NAME = "joinKeys";
    private static final String JOIN_TYPE_NAME = "joinType";
    private static final String SELECTED_FIELDS_DESC = "List of fields to be selected and/or renamed in the Joiner " +
      "output from each stages. There must not be a duplicate fields in the output.";
    private static final String JOIN_KEY_DESC = "List of join keys to perform join operation. The list is " +
      "separated by '&'. Join key from each input stage will be prefixed with '<stageName>.' And the " +
      "relation among join keys from different inputs is represented by '='. For example: " +
      "customers.customer_id=items.c_id&customers.customer_name=items.c_name means the join key is a composite key" +
      " of customer id and customer name from customers and items input stages and join will be performed on " +
      "equality of the join keys.";
    private static final String JOIN_TYPE_DESC = "The type of join to perform. Supports INNER, LEFT, RIGHT, FULL, " +
      "SEMI, and ANTI.";

    @Name(SELECTED_FIELDS_NAME)
    @Description(SELECTED_FIELDS_DESC)
    private final String selectedFields;

    @Name(JOIN_KEY_NAME)
    @Description(JOIN_KEY_DESC)
    private final String joinKeys;

    @Name(JOIN_TYPE_NAME)
    @Description(JOIN_TYPE_DESC)
    private final String joinType;

    public JoinerConfig(String selectedFields, String joinKeys, String joinType) {
      this.selectedFields = selectedFields;
      this.joinKeys = joinKeys;
      this.joinType = joinType;
    }

    public String getSelectedFields() {
      return selectedFields;
    }

    public String getJoinKeys() {
      return joinKeys;
    }

    public String getJoinType() {
      return joinType;
    }

    /**
     * Parses the join keys string from the config into {@link JoinKey} objects.
     */
    public List<JoinKey> parseJoinKeys() {
      List<JoinKey> joinKeyList = new ArrayList<>();

      Iterable<String> multipleJoinKeys = Splitter.on('&').trimResults().omitEmptyStrings().split(joinKeys);
      if (Iterables.isEmpty(multipleJoinKeys)) {
        throw new IllegalArgumentException("Individual join keys cannot be empty.");
      }

      for (String singleJoinKey : multipleJoinKeys) {
        String[] joinKeyArr = singleJoinKey.trim().split("\\s*=\\s*");
        if (joinKeyArr.length != 2 || Strings.isNullOrEmpty(joinKeyArr[0]) || Strings.isNullOrEmpty(joinKeyArr[1])) {
          throw new IllegalArgumentException("Each join key must have left hand and right hand operands.");
        }
        String leftOperand = joinKeyArr[0];
        String[] leftOpArr = leftOperand.split(Pattern.quote("."));
        if (leftOpArr.length != 2 || Strings.isNullOrEmpty(leftOpArr[0]) || Strings.isNullOrEmpty(leftOpArr[1])) {
          throw new IllegalArgumentException("Left join key operand must have a stage and a field name.");
        }
        String rightOperand = joinKeyArr[1];
        String[] rightOpArr = rightOperand.split(Pattern.quote("."));
        if (rightOpArr.length != 2 || Strings.isNullOrEmpty(rightOpArr[0]) || Strings.isNullOrEmpty(rightOpArr[1])) {
          throw new IllegalArgumentException("Right join key operand must have a stage and a field name.");
        }
        String leftStage = leftOpArr[0];
        String leftKey = leftOpArr[1];
        String rightStage = rightOpArr[0];
        String rightKey = rightOpArr[1];
        JoinKey key = new JoinKey(leftStage, leftKey, rightStage, rightKey);
        joinKeyList.add(key);
      }
      return joinKeyList;
    }

    /**
     * Parses the selected fields into a list.
     */
    public Map<String, String> parseSelectedFields() {
      Map<String, String> selectedFieldsMap = new LinkedHashMap<>();
      if (Strings.isNullOrEmpty(selectedFields)) {
        throw new IllegalArgumentException("Selected fields cannot be empty.");
      }
      String[] fields = selectedFields.trim().split("\\s*,\\s*");
      for (String field: fields) {
        String[] fieldAliasArr = field.split(Pattern.quote(" as "));
        if (fieldAliasArr.length != 2 || Strings.isNullOrEmpty(fieldAliasArr[0])
          || Strings.isNullOrEmpty(fieldAliasArr[1])) {
          throw new IllegalArgumentException("Selected fields must contain a field name and an alias.");
        }
        String fieldName = fieldAliasArr[0];
        String alias = fieldAliasArr[1];
        String[] fieldArr = fieldAliasArr[0].split(Pattern.quote("."));
        if (fieldArr.length != 2 || Strings.isNullOrEmpty(fieldArr[0]) || Strings.isNullOrEmpty(fieldArr[1])) {
          throw new IllegalArgumentException("Selected fields must contain a stage name and a field name.");
        }
        if (selectedFieldsMap.containsKey(fieldName)) {
          throw new IllegalArgumentException("Unable to rename a field twice.");
        }
        if (selectedFieldsMap.containsValue(alias)) {
          throw new IllegalArgumentException("Unable to rename two fields to the same name.");
        }
        selectedFieldsMap.put(fieldName, alias);
      }
      return selectedFieldsMap;
    }

    /**
     * Parses the join type into a Calcite {@link JoinRelType}.
     *
     * @return a Calcite join type
     */
    public JoinRelType parseJoinType() {
      try {
        JoinRelType joinRelType = JoinRelType.valueOf(joinType.toUpperCase());
        return joinRelType;
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Unsupported join type: " + joinType);
      }
    }

    /**
     * A simple class which represents a join key. Contains the stage and field of each operand and represents
     * an equals conditional.
     */
    private class JoinKey {
      private final String leftStage;
      private final String leftKey;
      private final String rightStage;
      private final String rightKey;

      private JoinKey(String leftStage, String leftKey, String rightStage, String rightKey) {
        this.leftStage = leftStage;
        this.leftKey = leftKey;
        this.rightStage = rightStage;
        this.rightKey = rightKey;
      }

      public String getLeftStage() {
        return leftStage;
      }

      public String getLeftKey() {
        return leftKey;
      }

      public String getRightStage() {
        return rightStage;
      }

      public String getRightKey() {
        return rightKey;
      }

      public String toString() {
        return String.format("%s.%s = %s.%s", leftStage, leftKey, rightStage, rightKey);
      }
    }
  }
}

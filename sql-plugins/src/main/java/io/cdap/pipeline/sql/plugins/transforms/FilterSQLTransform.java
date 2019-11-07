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

package io.cdap.pipeline.sql.plugins.transforms;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.pipeline.sql.api.template.SQLTransform;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import javax.annotation.Nullable;

/**
 * A SQL Filter transform.
 */
@Plugin(type = SQLTransform.PLUGIN_TYPE)
@Name("Filter")
@Description("The Filter transform lets you add a filter to an SQL query.")
public class FilterSQLTransform extends SQLTransform {
  private static final String LEFT_VALUE_DESC = "The value which forms the left-hand operand of the comparison.";
  private static final String LEFT_TYPE_DESC = "The type of the left-hand operand.";
  private static final String RIGHT_VALUE_DESC = "The value which forms the right-hand operand of the comparison.";
  private static final String RIGHT_TYPE_DESC = "The type of the right-hand operand.";
  private static final String OPERATION_DESC = "The operation for this filter.";

  /**
   * Config class for FilterSQLTransform
   */
  public static class FilterTransformTransformConfig extends PluginConfig {
    @Description(LEFT_VALUE_DESC)
    @Nullable
    String leftValue;

    @Description(LEFT_TYPE_DESC)
    @Nullable
    String leftType;

    @Description(RIGHT_VALUE_DESC)
    @Nullable
    String rightValue;

    @Description(RIGHT_TYPE_DESC)
    @Nullable
    String rightType;

    @Description(OPERATION_DESC)
    @Nullable
    String operation;

    public FilterTransformTransformConfig(String leftValue, String leftType,
                                          String rightValue, String rightType, String operation) {
      this.leftValue = leftValue;
      this.leftType = leftType;
      this.rightValue = rightValue;
      this.rightType = rightType;
      this.operation = operation;
    }

    public String getLeftValue() {
      return leftValue;
    }

    public String getLeftType() {
      return leftType;
    }

    public String getRightValue() {
      return rightValue;
    }

    public String getRightType() {
      return rightType;
    }

    public String getOperation() {
      return operation;
    }
  }

  private final FilterTransformTransformConfig config;

  public FilterSQLTransform(FilterTransformTransformConfig projectionTransformConfig) {
    this.config = projectionTransformConfig;
  }

  private SqlBinaryOperator getOperatorFromString(String operatorStr) {
    switch(operatorStr) {
      case "LESS_THAN":
        return SqlStdOperatorTable.LESS_THAN;
      case "LESS_THAN_OR_EQUAL":
        return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      case "EQUALS":
        return SqlStdOperatorTable.EQUALS;
      case "GREATER_THAN":
        return SqlStdOperatorTable.GREATER_THAN;
      case "GREATER_THAN_OR_EQUAL":
        return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
      default:
        throw new IllegalArgumentException("Invalid operator string type " + operatorStr);
    }
  }

  private RexNode getOperandFromStrings(RelBuilder builder, String value, String type) {
    switch(type) {
      case "FIELD":
        return builder.field(value);
      case "INTEGER":
        return builder.literal(Integer.valueOf(value));
      case "STRING":
        return builder.literal(value);
      default:
        throw new IllegalArgumentException("Unsupported operand type " + type);
    }
  }

  @Override
  public RelNode getQuery(RelBuilder builder) {
    if (Strings.isNullOrEmpty(config.getLeftValue())) {
      throw new IllegalArgumentException("Must specify a left-hand value.");
    }
    if (Strings.isNullOrEmpty(config.getLeftType())) {
      throw new IllegalArgumentException("Must specify a left-hand type.");
    }
    if (Strings.isNullOrEmpty(config.getRightValue())) {
      throw new IllegalArgumentException("Must specify a right-hand value.");
    }
    if (Strings.isNullOrEmpty(config.getRightType())) {
      throw new IllegalArgumentException("Must specify a right-hand type.");
    }
    if (Strings.isNullOrEmpty(config.getOperation())) {
      throw new IllegalArgumentException("Must specify an operation.");
    }
    RexNode leftOperand = getOperandFromStrings(builder, config.getLeftValue(), config.getLeftType().toUpperCase());
    RexNode rightOperand = getOperandFromStrings(builder, config.getRightValue(), config.getRightType().toUpperCase());
    SqlBinaryOperator operator = getOperatorFromString(config.getOperation().toUpperCase());
    builder.filter(builder.call(operator, leftOperand, rightOperand));
    return builder.build();
  }
}

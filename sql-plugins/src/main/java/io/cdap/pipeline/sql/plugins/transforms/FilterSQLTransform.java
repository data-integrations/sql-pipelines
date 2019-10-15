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
import io.cdap.pipeline.sql.api.core.Column;
import io.cdap.pipeline.sql.api.core.Filter;
import io.cdap.pipeline.sql.api.core.StructuredQuery;
import io.cdap.pipeline.sql.api.core.constants.DateTimeConstant;
import io.cdap.pipeline.sql.api.core.constants.IntegerConstant;
import io.cdap.pipeline.sql.api.core.constants.StringConstant;
import io.cdap.pipeline.sql.api.core.enums.ConstantType;
import io.cdap.pipeline.sql.api.core.enums.OperandType;
import io.cdap.pipeline.sql.api.core.enums.PredicateOperatorType;
import io.cdap.pipeline.sql.api.core.interfaces.Operand;
import io.cdap.pipeline.sql.api.template.SQLNode;

import java.time.LocalDateTime;
import javax.annotation.Nullable;

/**
 * A SQL Projection transform.
 */
@Plugin(type = SQLNode.PLUGIN_TYPE)
@Name("Filter")
@Description("The Projection transform lets you add a filter to an SQL query.")
public class FilterSQLTransform extends SQLNode {
  private static final String LEFT_VALUE_DESC = "The value which forms the left-hand operand of the comparison.";
  private static final String LEFT_TYPE_DESC = "The type of the left-hand operand.";
  private static final String RIGHT_VALUE_DESC = "The value which forms the right-hand operand of the comparison.";
  private static final String RIGHT_TYPE_DESC = "The type of the right-hand operand.";
  private static final String OPERATION_DESC = "The operation for this filter.";

  /**
   * Config class for ProjectionTransform
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

  @Override
  public StructuredQuery constructQuery() {
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
    PredicateOperatorType operator;
    try {
      operator = PredicateOperatorType.valueOf(config.getOperation().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported operation type " + config.getOperation());
    }
    if (operator.equals(PredicateOperatorType.OR) || operator.equals(PredicateOperatorType.AND)) {
      throw new IllegalArgumentException("Nested operator types are currently unsupported.");
    }
    // Setup the builder
    StructuredQuery.Builder builder = StructuredQuery.builder();
    Filter filter = new Filter(getOperand(config.getLeftValue(), config.getLeftType()),
                               getOperand(config.getRightValue(), config.getRightType()), operator);
    builder.where(filter);
    return builder.build();
  }

  private Operand getOperand(String value, String type) {
    OperandType typeEnum;
    try {
      typeEnum = OperandType.valueOf(type.toUpperCase());
    } catch (IllegalArgumentException e) {
      // Probably a constant type, try returning that
      return getConstantOperand(value, type);
    }
    if (typeEnum.equals(OperandType.COLUMN)) {
      return Column.builder(value).build();
    }
    // Constant type but unspecified
    throw new IllegalArgumentException("Unsupported operand type " + type);
  }

  private Operand getConstantOperand(String value, String type) {
    ConstantType typeEnum = ConstantType.valueOf(type.toUpperCase());
    switch(typeEnum) {
      case DATETIME:
        return new DateTimeConstant(LocalDateTime.parse(value));
      case INTEGER:
        return new IntegerConstant(Integer.parseInt(value));
      case STRING:
        return new StringConstant(value);
      default:
        throw new IllegalArgumentException("Unsupported constant type " + type);
    }
  }
}

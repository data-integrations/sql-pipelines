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

package io.cdap.pipeline.sql.api;


import io.cdap.pipeline.sql.api.enums.SQLPredicateOperator;
import io.cdap.pipeline.sql.api.interfaces.Operand;

import javax.annotation.Nullable;

/**
 * A SQL component which represents a filter expression.
 *
 * The filter expression can be represented as an operation expression tree.
 */
public class SQLFilter extends ExpressionTreeNode<Operand, SQLPredicateOperator> {
  public SQLFilter(Operand left, Operand right, SQLPredicateOperator operator) {
    super(null, null, left, right, operator);
  }

  public SQLFilter(SQLFilter left, SQLFilter right, SQLPredicateOperator operator) {
    super(left, right, null, null, operator);
  }

  @Nullable
  public SQLFilter getLeftFilter() {
    return (SQLFilter) super.getLeftNode();
  }

  @Nullable
  public SQLFilter getRightFilter() {
    return (SQLFilter) super.getRightNode();
  }
}

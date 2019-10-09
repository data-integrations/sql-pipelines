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


import io.cdap.pipeline.sql.api.enums.PredicateOperatorType;
import io.cdap.pipeline.sql.api.interfaces.Operand;

import javax.annotation.Nullable;

/**
 * A SQL component which represents a filter expression.
 *
 * The filter expression can be represented as an operation expression tree.
 */
public class Filter extends ExpressionTreeNode<Operand, PredicateOperatorType> {
  public Filter(Operand left, Operand right, PredicateOperatorType operator) {
    super(null, null, left, right, operator);
  }

  public Filter(Filter left, Filter right, PredicateOperatorType operator) {
    super(left, right, null, null, operator);
  }

  @Nullable
  public Filter getLeftFilter() {
    return (Filter) super.getLeftNode();
  }

  @Nullable
  public Filter getRightFilter() {
    return (Filter) super.getRightNode();
  }
}

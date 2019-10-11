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

package io.cdap.pipeline.sql.api.core;

import javax.annotation.Nullable;

class ExpressionTreeNode<T, O> {
  private final ExpressionTreeNode<T, O> leftNode;
  private final ExpressionTreeNode<T, O> rightNode;
  private final T leftOperand;
  private final T rightOperand;
  private final O operation;

  protected ExpressionTreeNode(@Nullable ExpressionTreeNode<T, O> leftNode,
                               @Nullable ExpressionTreeNode<T, O> rightNode,
                               @Nullable T leftOperand, @Nullable T rightOperand, O operation) {
    this.leftNode = leftNode;
    this.rightNode = rightNode;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
    this.operation = operation;
  }

  @Nullable
  protected ExpressionTreeNode<T, O> getLeftNode() {
    return leftNode;
  }

  @Nullable
  protected ExpressionTreeNode<T, O> getRightNode() {
    return rightNode;
  }

  @Nullable
  public T getLeftOperand() {
    return leftOperand;
  }

  @Nullable
  public T getRightOperand() {
    return rightOperand;
  }

  public O getOperation() {
    return operation;
  }

  public boolean isLeaf() {
    return leftNode == null && rightNode == null;
  }
}

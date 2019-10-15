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

package io.cdap.pipeline.sql.api.core.enums;

import io.cdap.pipeline.sql.api.core.constants.DateTimeConstant;
import io.cdap.pipeline.sql.api.core.constants.IntegerConstant;
import io.cdap.pipeline.sql.api.core.constants.StringConstant;

/**
 * An enum representing the constant types for the {@link io.cdap.pipeline.sql.api.core.interfaces.Constant} interface.
 */
public enum ConstantType {
  DATETIME(DateTimeConstant.class), INTEGER(IntegerConstant.class), STRING(StringConstant.class);

  private final Class constantClass;

  ConstantType(Class constantClass) {
    this.constantClass = constantClass;
  }

  public Class getConstantClass() {
    return constantClass;
  }
}

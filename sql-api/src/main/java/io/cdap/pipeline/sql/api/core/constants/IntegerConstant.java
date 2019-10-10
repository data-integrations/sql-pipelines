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

package io.cdap.pipeline.sql.api.core.constants;

import io.cdap.pipeline.sql.api.core.enums.ConstantType;

/**
 * A constant wrapping a primitive {@link Integer} type.
 */
public class IntegerConstant extends AbstractConstant<Integer> {
  public IntegerConstant(Integer i) {
    super(i);
  }

  @Override
  public ConstantType getConstantType() {
    return ConstantType.INTEGER;
  }
}

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

package io.cdap.pipeline.sql.api.constants;

import io.cdap.pipeline.sql.api.interfaces.Constant;

/**
 * An abstract constant type.
 * @param <T> The type of object to encapsulate
 */
public abstract class AbstractConstant<T> implements Constant<T> {
  private final T value;

  public AbstractConstant(T v) {
    this.value = v;
  }

  public T get() {
    return value;
  }
}
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

package io.cdap.pipeline.sql.app.core;

import io.cdap.cdap.etl.proto.v2.ETLConfig;

import java.util.HashMap;
import java.util.HashSet;
import javax.annotation.Nullable;

/**
 * Represents an SQL configuration object.
 *
 * Currently, the SQLConfig object represents a direct extension of {@link ETLConfig}, but future configuration
 * variables may be added.
 */
public class SQLConfig extends ETLConfig {
  private final String schedule;

  /**
   * For compilation purposes.
   */
  public SQLConfig(String schedule) {
    super(new HashSet<>(), new HashSet<>(), null, null, null,
          false, false, 0, new HashMap<>());
    this.schedule = schedule;
  }

  @Nullable
  public String getSchedule() {
    return schedule;
  }
}

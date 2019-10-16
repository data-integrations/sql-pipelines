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

package io.cdap.pipeline.sql.app;

import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.pipeline.sql.api.template.interfaces.QueryConfigurable;
import io.cdap.pipeline.sql.app.core.interfaces.PluginMapper;

import java.util.Map;

public class MockPluginMapper implements PluginMapper {
  private int count;
  private final Map<String, QueryConfigurable> pluginMap;

  public MockPluginMapper(Map<String, QueryConfigurable> pluginMap) {
    this.pluginMap = pluginMap;
  }

  @Override
  public QueryConfigurable getMappedPluginInstance(ETLPlugin plugin, String stageName) {
    count++;
    return pluginMap.get(stageName);
  }

  public int getPluginCount() {
    return count;
  }
}

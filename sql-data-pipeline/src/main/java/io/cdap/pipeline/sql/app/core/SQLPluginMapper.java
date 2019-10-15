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

import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.InvalidPluginProperty;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.etl.common.ArtifactSelectorProvider;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.spec.TrackedPluginSelector;
import io.cdap.pipeline.sql.api.template.interfaces.QueryConfigurable;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts {@link io.cdap.cdap.etl.proto.v2.ETLPlugin} to their respective plugins using a simple switch statement.
 */
public class SQLPluginMapper {
  private final PluginConfigurer pluginConfigurer;

  public SQLPluginMapper(PluginConfigurer configurer) {
    this.pluginConfigurer = configurer;
  }

  public QueryConfigurable getMappedPluginInstance(ETLPlugin plugin, String stageName) {
    TrackedPluginSelector pluginSelector = new TrackedPluginSelector(
      new ArtifactSelectorProvider().getPluginSelector(plugin.getArtifactConfig()));
    Object pluginObject = getPlugin(stageName, plugin, pluginSelector, plugin.getType(), plugin.getName());
    return (QueryConfigurable) pluginObject;
  }

  /**
   * Adds a Plugin usage to the Application and create a new instance.
   *
   * @param stageName stage name
   * @param etlPlugin plugin
   * @param pluginSelector plugin selector
   * @param type type of the plugin
   * @param pluginName plugin name
   * @throws IllegalArgumentException if error while creating new plugin instance
   * @throws IllegalStateException if no plugin artifact found
   * @return new instance of the plugin
   */
  private Object getPlugin(String stageName, ETLPlugin etlPlugin, TrackedPluginSelector pluginSelector, String type,
                           String pluginName) {
    Object plugin;
    try {
      // Call to usePlugin may throw IllegalArgumentException if hte plugin with the same id is already deployed.
      // This would mean there is a bug in the app and this can not be fixed by user. That is why it is not handled as
      // a ValidationFailure.
      plugin = pluginConfigurer.usePlugin(type, pluginName,
                                          stageName, etlPlugin.getPluginProperties(), pluginSelector);
    } catch (InvalidPluginConfigException e) {
      List<String> missingInvalidProperties = new ArrayList<>();
      for (String missingProperty : e.getMissingProperties()) {
        missingInvalidProperties.add(missingProperty);

      }
      for (InvalidPluginProperty invalidProperty: e.getInvalidProperties()) {
        missingInvalidProperties.add(invalidProperty.getName());
      }
      throw new IllegalArgumentException(String.format("Properties '%s' are missing or invalid.",
                                                       missingInvalidProperties));
    }

    if (plugin == null) {
      String errorMessage = String.format("Plugin named '%s' of type '%s' not found.", pluginName, type);
      throw new IllegalStateException(errorMessage);
    }
    return plugin;
  }
}

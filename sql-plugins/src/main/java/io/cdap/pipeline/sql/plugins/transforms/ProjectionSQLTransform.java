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
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.pipeline.sql.api.template.SQLTransform;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * A SQL Projection transform.
 */
@Plugin(type = SQLTransform.PLUGIN_TYPE)
@Name("Projection")
@Description("The Projection transform lets you drop, select, rename, and cast fields to a different type.")
public class ProjectionSQLTransform extends SQLTransform {
  private static final String RENAME_DESC = "List of fields to rename. This is a comma-separated list of key-value " +
    "pairs, where each pair is separated by a colon and specifies the input and output names. For " +
    "example: 'datestr:date,timestamp:ts' specifies that the 'datestr' field should be renamed to 'date' and the " +
    "'timestamp' field should be renamed to 'ts'.";
  private static final String CAST_DESC = "List of fields to cast to a different type. This is a comma-" +
    "separated list of key-value pairs, where each pair is separated by a colon and specifies the field name and " +
    "the desired type.";
  private static final String SELECT_DESC = "Comma-separated list of fields to select. For example: " +
    "'field1,field2,field3'.";

  /**
   * Config class for ProjectionTransform
   */
  public static class ProjectionSQLTransformConfig extends PluginConfig {
    @Description(RENAME_DESC)
    @Nullable
    String rename;

    @Description(CAST_DESC)
    @Nullable
    String cast;

    @Description(SELECT_DESC)
    @Nullable
    String select;
    public ProjectionSQLTransformConfig(String rename, String cast, String select) {
      this.rename = rename;
      this.cast = cast;
      this.select = select;
    }
  }

  private final ProjectionSQLTransformConfig projectionTransformConfig;
  private final List<String> fieldsToSelect;
  private final BiMap<String, String> fieldsToRename;
  private final Map<String, SqlTypeName> fieldsToCast;

  public ProjectionSQLTransform(ProjectionSQLTransformConfig projectionTransformConfig) {
    this.projectionTransformConfig = projectionTransformConfig;
    this.fieldsToSelect = new ArrayList<>();
    this.fieldsToRename = HashBiMap.create();
    this.fieldsToCast = new HashMap<>();
  }

  @Override
  public RelNode getQuery(RelBuilder builder) {
    // Initialize
    init();
    if (fieldsToSelect.isEmpty()) {
      throw new IllegalArgumentException("Must specify what columns to select.");
    }
    List<RexNode> fields = new ArrayList<>();
    for (String colStr : fieldsToSelect) {
      RexNode field = builder.field(colStr);
      if (fieldsToCast.containsKey(colStr)) {
        SqlTypeName castType = fieldsToCast.get(colStr);
        field = builder.cast(field, castType);
      }
      if (fieldsToRename.containsKey(colStr)) {
        String alias = fieldsToRename.get(colStr);
        field = builder.alias(field, alias);
      }
      fields.add(field);
    }
    return builder.project(fields).build();
  }

  private void init() {
    // Setup select sets
    if (!Strings.isNullOrEmpty(projectionTransformConfig.select)) {
      for (String field: projectionTransformConfig.select.split(Pattern.quote(","))) {
        if (!Strings.isNullOrEmpty(field)) {
          fieldsToSelect.add(field);
        } else {
          throw new IllegalArgumentException("Select field may not be empty.");
        }
      }
    } else {
      throw new IllegalArgumentException("Must specify one of drop or select.");
    }
    // Setup the rename map
    if (!Strings.isNullOrEmpty(projectionTransformConfig.rename)) {
      String[] mappings = projectionTransformConfig.rename.split(Pattern.quote(","));
      for (String mapping: mappings) {
        String[] keyValuePair = mapping.split(Pattern.quote(":"));
        if (keyValuePair.length != 2) {
          throw new IllegalArgumentException("Rename mapping '" + mapping + "' must contain a key and a value.");
        }
        if (!fieldsToSelect.contains(keyValuePair[0])) {
          throw new IllegalArgumentException("Field to rename must be present in selected columns.");
        }
        if (fieldsToRename.containsKey(keyValuePair[0])) {
          throw new IllegalArgumentException("Cannot rename column '" +
                                               keyValuePair[0] + "' to two different values.");
        }
        try {
          fieldsToRename.put(keyValuePair[0], keyValuePair[1]);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Cannot rename more than one column to '" + keyValuePair[1] + " " +
                                               e.getMessage());
        }
      }
    }
    // Setup the cast map
    if (!Strings.isNullOrEmpty(projectionTransformConfig.cast)) {
      String[] mappings = projectionTransformConfig.cast.split(Pattern.quote(","));
      for (String mapping: mappings) {
        String[] keyValuePair = mapping.split(Pattern.quote(":"));
        if (keyValuePair.length != 2) {
          throw new IllegalArgumentException("Cast mapping '" + mapping + "' must contain a key and a type.");
        }
        if (!fieldsToSelect.contains(keyValuePair[0])) {
          throw new IllegalArgumentException("Field to cast must be present in selected columns.");
        }
        SqlTypeName type = SqlTypeName.get(keyValuePair[1].toUpperCase());
        if (type == null) {
          throw new IllegalArgumentException("Unsupported type cast to '" + keyValuePair[1] + "'.");
        }
        if (fieldsToCast.containsKey(keyValuePair[0])) {
          throw new IllegalArgumentException("Cannot cast column '" + keyValuePair[0] + "' to multiple types.");
        }
        fieldsToCast.put(keyValuePair[0], type);
      }
    }
  }
}

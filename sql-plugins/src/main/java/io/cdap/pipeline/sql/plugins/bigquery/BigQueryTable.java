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

package io.cdap.pipeline.sql.plugins.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * An adapter implementation for a BigQuery table.
 */
public class BigQueryTable extends AbstractTable {
  private final Schema bigQuerySchema;

  public BigQueryTable(Schema bigQuerySchema) {
    this.bigQuerySchema = bigQuerySchema;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<RelDataType> colTypes = new ArrayList<>();
    List<String> colNames = new ArrayList<>();
    for (Field field: bigQuerySchema.getFields()) {
      colTypes.add(createSubType(field, typeFactory));
      colNames.add(field.getName());
    }
    return typeFactory.createStructType(colTypes, colNames);
  }

  private RelDataType createSubType(Field field, RelDataTypeFactory typeFactory) {
    // Convert from BigQuery type to Calcite type
    SqlTypeName calciteType = convertBasicType(field.getType().getStandardType());
    RelDataType subType;
    if (calciteType.equals(SqlTypeName.ARRAY)) {
      // ARRAY type
      subType = typeFactory.createArrayType(createSubType(field.getSubFields().get(0), typeFactory), -1);
    } else if (field.getMode().equals(Field.Mode.REPEATED)) {
      // REPEATED type (basically ARRAY)
      subType = typeFactory.createArrayType(typeFactory.createSqlType(calciteType), -1);
    } else if (calciteType.equals(SqlTypeName.STRUCTURED)) {
      // STRUCT type
      List<RelDataType> fieldTypes = new ArrayList<>();
      List<String> fieldNames = new ArrayList<>();
      for (Field subField: field.getSubFields()) {
        fieldTypes.add(createSubType(subField, typeFactory));
        fieldNames.add(subField.getName());
      }
      subType = typeFactory.createStructType(fieldTypes, fieldNames);
    } else {
      // Basic SQL types
      subType = typeFactory.createSqlType(calciteType);
    }
    // Check if the type is nullable
    return typeFactory.createTypeWithNullability(subType, field.getMode().equals(Field.Mode.NULLABLE));
  }

  private SqlTypeName convertBasicType(StandardSQLTypeName type) {
    switch(type) {
      case DATE:
        return SqlTypeName.DATE;
      case BOOL:
        return SqlTypeName.BOOLEAN;
      case DATETIME:
        return SqlTypeName.TIMESTAMP;
      case TIME:
        return SqlTypeName.TIME;
      case ARRAY:
        return SqlTypeName.ARRAY;
      case BYTES:
        return SqlTypeName.VARBINARY;
      case INT64:
        return SqlTypeName.INTEGER;
      case STRING:
        return SqlTypeName.VARCHAR;
      case STRUCT:
        return SqlTypeName.STRUCTURED;
      case FLOAT64:
        return SqlTypeName.FLOAT;
      case NUMERIC:
        return SqlTypeName.DECIMAL;
      default:
        throw new IllegalArgumentException("Unsupported type " + type.toString());
    }
  }
}

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

package io.cdap.pipeline.sql.app.core.adapters;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.pipeline.sql.api.core.enums.OperandType;
import io.cdap.pipeline.sql.api.core.interfaces.Operand;

import java.lang.reflect.Type;

/**
 * An adapter to perform JSON serialization and deserialization of an {@link Operand}.
 */
public class OperandAdapter implements JsonSerializer<Operand>, JsonDeserializer<Operand> {
  private static final String OPERAND_TYPE_NAME = "operandType";
  private static final String INSTANCE_DATA_NAME = "instanceData";

  @Override
  public JsonElement serialize(final Operand from, final Type fromType, JsonSerializationContext context) {
    JsonObject obj = new JsonObject();
    OperandType type = from.getOperandType();
    obj.add(OPERAND_TYPE_NAME, context.serialize(type));
    // Class parameter is necessary since an operand can be a constant and its type needs to be serialized too
    obj.add(INSTANCE_DATA_NAME, context.serialize(from, type.getOperandClass()));
    return obj;
  }

  @Override
  public Operand deserialize(final JsonElement from, final Type fromType, JsonDeserializationContext context) {
    JsonObject obj = from.getAsJsonObject();
    OperandType type = context.deserialize(obj.get(OPERAND_TYPE_NAME), OperandType.class);
    return context.deserialize(obj.get(INSTANCE_DATA_NAME), type.getOperandClass());
  }
}

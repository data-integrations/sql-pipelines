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

import io.cdap.pipeline.sql.api.core.Filter;

/**
 * An enum representing the different operation types in an {@link Filter} expression.
 */
public enum PredicateOperatorType {
  LESS, LESS_OR_EQUAL, EQUAL, GREATER, GREATER_OR_EQUAL, AND, OR
}

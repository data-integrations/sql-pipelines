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

import io.cdap.pipeline.sql.api.core.StructuredQuery;
import io.cdap.pipeline.sql.api.core.Table;
import io.cdap.pipeline.sql.api.template.SQLSink;

public class MockPlugin extends SQLSink {
  private int constructCount;
  private int destinationTableCount;
  private final StructuredQuery query;
  private final Table destinationTable;

  public MockPlugin(StructuredQuery query, Table destinationTable) {
    this.query = query;
    this.destinationTable = destinationTable;
  }

  @Override
  public StructuredQuery constructQuery() {
    constructCount++;
    return query;
  }

  public Table getDestinationTable() {
    destinationTableCount++;
    return destinationTable;
  }

  public int getConstructCount() {
    return constructCount;
  }

  public int getDestinationTableCount() {
    return destinationTableCount;
  }
}

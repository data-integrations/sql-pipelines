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

import io.cdap.pipeline.sql.api.template.tables.AbstractTableInfo;
import org.junit.Assert;
import org.junit.Test;

public class BigQueryTest {
  @Test(expected = IllegalArgumentException.class)
  public void testEmptySourceProject() {
    BigQuerySQLSource.BigQuerySQLSourceConfig config = new BigQuerySQLSource.BigQuerySQLSourceConfig("", "a",
                                                                                                 "b", "c");
    BigQuerySQLSource source = new BigQuerySQLSource(config);
    source.getSourceTable();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptySourceDataset() {
    BigQuerySQLSource.BigQuerySQLSourceConfig config = new BigQuerySQLSource.BigQuerySQLSourceConfig("a", "",
                                                                                             "b", "c");
    BigQuerySQLSource source = new BigQuerySQLSource(config);
    source.getSourceTable();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptySourceTable() {
    BigQuerySQLSource.BigQuerySQLSourceConfig config = new BigQuerySQLSource.BigQuerySQLSourceConfig("a", "b",
                                                                                             "", "c");
    BigQuerySQLSource source = new BigQuerySQLSource(config);
    source.getSourceTable();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptySinkProject() {
    BigQuerySQLSink.BigQuerySQLSinkConfig config = new BigQuerySQLSink.BigQuerySQLSinkConfig("", "a",
                                                                                             "b", "c");
    BigQuerySQLSink sink = new BigQuerySQLSink(config);
    sink.getDestinationTable();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptySinkDataset() {
    BigQuerySQLSink.BigQuerySQLSinkConfig config = new BigQuerySQLSink.BigQuerySQLSinkConfig("a", "",
                                                                                             "b", "c");
    BigQuerySQLSink sink = new BigQuerySQLSink(config);
    sink.getDestinationTable();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptySinkTable() {
    BigQuerySQLSink.BigQuerySQLSinkConfig config = new BigQuerySQLSink.BigQuerySQLSinkConfig("a", "b",
                                                                                             "", "c");
    BigQuerySQLSink sink = new BigQuerySQLSink(config);
    sink.getDestinationTable();
  }

  @Test
  public void testValidSinkConfig() {
    BigQuerySQLSink.BigQuerySQLSinkConfig config = new BigQuerySQLSink.BigQuerySQLSinkConfig("a", "b",
                                                                                             "c", "d");
    BigQuerySQLSink sink = new BigQuerySQLSink(config);
    AbstractTableInfo tableInfo = sink.getDestinationTable();
    Assert.assertEquals("a.b.c", tableInfo.getTableName());
  }
}

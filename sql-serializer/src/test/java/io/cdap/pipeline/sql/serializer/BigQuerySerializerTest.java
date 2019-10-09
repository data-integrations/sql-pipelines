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

package io.cdap.pipeline.sql.serializer;

import io.cdap.pipeline.sql.api.Column;
import io.cdap.pipeline.sql.api.Filter;
import io.cdap.pipeline.sql.api.StructuredQuery;
import io.cdap.pipeline.sql.api.Table;
import io.cdap.pipeline.sql.api.constants.IntegerConstant;
import io.cdap.pipeline.sql.api.constants.StringConstant;
import io.cdap.pipeline.sql.api.enums.PredicateOperatorType;
import io.cdap.pipeline.sql.serializer.bigquery.BigQuerySQLSerializer;
import org.junit.Assert;
import org.junit.Test;

public class BigQuerySerializerTest {
  @Test(expected = IllegalArgumentException.class)
  public void testNullQuery() {
    Table temp = new Table(null, null, "a", null);
    BigQuerySQLSerializer serializer = new BigQuerySQLSerializer();
    serializer.getSQL(null, temp);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidQualifiedTemporaryTable() {
    StringConstant stringConstant = new StringConstant("test1");

    Table table = new Table("a", "b", "c", "d");
    Column col1 = new Column("col1", table, "c1");
    Column col2 = new Column("col2", table, null);
    Filter filter = new Filter(col1, stringConstant, PredicateOperatorType.EQUAL);
    StructuredQuery validQuery = StructuredQuery.builder()
      .select(col1, col2).from(table).where(filter).build();

    BigQuerySQLSerializer serializer = new BigQuerySQLSerializer();
    Table temp = new Table("a", null, "b", null);
    serializer.getSQL(validQuery, temp);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAliasedTemporaryTable() {
    StringConstant stringConstant = new StringConstant("test1");

    Table table = new Table("a", "b", "c", "d");
    Column col1 = new Column("col1", table, "c1");
    Column col2 = new Column("col2", table, null);
    Filter filter = new Filter(col1, stringConstant, PredicateOperatorType.EQUAL);
    StructuredQuery validQuery = StructuredQuery.builder()
      .select(col1, col2).from(table).where(filter).build();

    BigQuerySQLSerializer serializer = new BigQuerySQLSerializer();
    Table temp = new Table(null, null, "b", "c");
    serializer.getSQL(validQuery, temp);
  }

  @Test
  public void testSimpleQuery() {
    final String expectedSimpleQueryString =
      "CREATE TEMPORARY TABLE `t1` AS SELECT d.col1 AS c1, d.col2 FROM `a.b.c` AS d WHERE (d.col1 = \"test1\");";

    StringConstant stringConstant = new StringConstant("test1");

    Table table = new Table("a", "b", "c", "d");
    Column col1 = new Column("col1", table, "c1");
    Column col2 = new Column("col2", table, null);
    Filter filter = new Filter(col1, stringConstant, PredicateOperatorType.EQUAL);
    StructuredQuery validQuery = StructuredQuery.builder()
      .select(col1, col2).from(table).where(filter).build();

    Table temporaryTable = new Table(null, null, "t1", null);

    BigQuerySQLSerializer serializer = new BigQuerySQLSerializer();
    Assert.assertEquals(expectedSimpleQueryString, serializer.getSQL(validQuery, temporaryTable));
  }

  @Test
  public void testComplexQuery() {
    final String expectedNestedQueryString =
      "CREATE TEMPORARY TABLE `t1` AS SELECT t2.c1 AS cn1, t2.col2 FROM (SELECT d.col1 AS c1, d.col2 FROM `a.b.c` " +
        "AS d WHERE (d.col1 = \"test1\")) AS t2 WHERE ((t2.c1 = \"test2\") OR (t2.col2 >= 15));";

    StringConstant innerString = new StringConstant("test1");
    StringConstant outerString = new StringConstant("test2");
    IntegerConstant intConstant = new IntegerConstant(15);

    Table table = new Table("a", "b", "c", "d");
    Column col1 = new Column("col1", table, "c1");
    Column col2 = new Column("col2", table, null);
    Filter innerFilter = new Filter(col1, innerString, PredicateOperatorType.EQUAL);
    StructuredQuery innerQuery = StructuredQuery.builder()
      .select(col1, col2).from(table).where(innerFilter).as("t2").build();

    Column outerCol1 = new Column("c1", innerQuery, "cn1");
    Column outerCol2 = new Column("col2", innerQuery, null);
    Filter subFilter1 = new Filter(outerCol1, outerString, PredicateOperatorType.EQUAL);
    Filter subFilter2 = new Filter(outerCol2, intConstant, PredicateOperatorType.GREATER_OR_EQUAL);
    Filter outerFilter = new Filter(subFilter1, subFilter2, PredicateOperatorType.OR);
    StructuredQuery complexQuery = StructuredQuery.builder()
      .select(outerCol1, outerCol2).from(innerQuery).where(outerFilter).build();

    Table temporaryTable = new Table(null, null, "t1", null);

    BigQuerySQLSerializer serializer = new BigQuerySQLSerializer();
    String queryString = serializer.getSQL(complexQuery, temporaryTable);
    Assert.assertEquals(expectedNestedQueryString, queryString);
  }

  @Test
  public void testComplexNoInnerTableAliasQuery() {
    final String expectedNoInnerTableAliasQueryString =
      "CREATE TEMPORARY TABLE `t1` AS SELECT t2.c1 AS cn1, t2.col2 FROM (SELECT col1 AS c1, col2 FROM `a.b.c` " +
        "WHERE (col1 = \"test1\")) AS t2 WHERE ((t2.c1 = \"test2\") OR (t2.col2 >= 15));";

    StringConstant innerString = new StringConstant("test1");
    StringConstant outerString = new StringConstant("test2");
    IntegerConstant intConstant = new IntegerConstant(15);

    Table innerTable = new Table("a", "b", "c", null);
    Column col1 = new Column("col1", innerTable, "c1");
    Column col2 = new Column("col2", innerTable, null);
    Filter innerFilter = new Filter(col1, innerString, PredicateOperatorType.EQUAL);
    StructuredQuery innerQuery = StructuredQuery.builder().select(col1, col2)
      .from(innerTable).where(innerFilter).as("t2").build();

    Column outerCol1 = new Column("c1", innerQuery, "cn1");
    Column outerCol2 = new Column("col2", innerQuery, null);
    Filter subFilter1 = new Filter(outerCol1, outerString, PredicateOperatorType.EQUAL);
    Filter subFilter2 = new Filter(outerCol2, intConstant, PredicateOperatorType.GREATER_OR_EQUAL);
    Filter outerFilter = new Filter(subFilter1, subFilter2, PredicateOperatorType.OR);
    StructuredQuery complexQuery = StructuredQuery.builder().select(outerCol1, outerCol2)
      .from(innerQuery).where(outerFilter).build();

    Table temporaryTable = new Table(null, null, "t1", null);

    BigQuerySQLSerializer serializer = new BigQuerySQLSerializer();
    String queryString = serializer.getSQL(complexQuery, temporaryTable);
    Assert.assertEquals(expectedNoInnerTableAliasQueryString, queryString);
  }
}

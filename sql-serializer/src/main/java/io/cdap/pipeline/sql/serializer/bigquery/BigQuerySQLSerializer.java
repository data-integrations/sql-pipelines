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

package io.cdap.pipeline.sql.serializer.bigquery;

import io.cdap.pipeline.sql.api.Column;
import io.cdap.pipeline.sql.api.Filter;
import io.cdap.pipeline.sql.api.StructuredQuery;
import io.cdap.pipeline.sql.api.Table;
import io.cdap.pipeline.sql.api.constants.DateTimeConstant;
import io.cdap.pipeline.sql.api.constants.IntegerConstant;
import io.cdap.pipeline.sql.api.constants.StringConstant;
import io.cdap.pipeline.sql.api.enums.OperandType;
import io.cdap.pipeline.sql.api.enums.PredicateOperatorType;
import io.cdap.pipeline.sql.api.enums.QueryableType;
import io.cdap.pipeline.sql.api.interfaces.Aliasable;
import io.cdap.pipeline.sql.api.interfaces.Constant;
import io.cdap.pipeline.sql.api.interfaces.Operand;
import io.cdap.pipeline.sql.api.interfaces.Queryable;
import io.cdap.pipeline.sql.serializer.interfaces.SQLSerializer;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/**
 * Serializes a {@link StructuredQuery} into a {@link String} able to be executed upon Google BigQuery.
 */
public class BigQuerySQLSerializer implements SQLSerializer {
  /**
   * Generates a BigQuery SQL statement string from the given query.
   * @param query The query to generate a string from
   * @return The generated query string
   */
  public String getSQL(StructuredQuery query) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null");
    }
    if (query.hasAlias()) {
      throw new IllegalArgumentException("Cannot have an alias on the main outer query");
    }
    StringBuilder b = new StringBuilder();
    appendQuery(b, query);
    b.append(';');
    return b.toString();
  }

  /**
   * Generates a BigQuery SQL statement string from the given query while also creating a temporary table from the
   * results of that query. Creating a temporary table takes the form of:
   *
   * CREATE TEMPORARY TABLE [{@link Table}] AS [{@link StructuredQuery}];
   *
   * Example return from creating a temporary table temp from selecting column CustomerName from a table Customers:
   * CREATE TEMPORARY TABLE temp AS SELECT CustomerName FROM Customers;
   *
   * @param query The query to generate a string from
   * @param temporaryTable The temporary table to create
   * @return The generated query string
   */
  public String getSQL(StructuredQuery query, Table temporaryTable) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null");
    }
    if (query.hasAlias()) {
      throw new IllegalArgumentException("Cannot have an alias on the main outer query");
    }
    if (temporaryTable.hasAlias()) {
      throw new IllegalArgumentException("Cannot have alias on temporary table");
    }
    if (temporaryTable.hasProject() || temporaryTable.hasDatabase()) {
      throw new IllegalArgumentException("Temporary tables cannot be fully qualified");
    }
    StringBuilder b = new StringBuilder();
    b.append("CREATE TEMPORARY TABLE ");
    appendTable(b, temporaryTable);
    b.append(" AS ");
    appendQuery(b, query);
    b.append(';');
    return b.toString();
  }

  public String getInsertIntoSQL(StructuredQuery query, Table destinationTable) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null");
    }
    if (query.hasAlias()) {
      throw new IllegalArgumentException("Cannot have an alias on the main outer query");
    }
    StringBuilder b = new StringBuilder();
    b.append("INSERT INTO ");
    appendTable(b, destinationTable);
    b.append(' ');
    appendQuery(b, query);
    b.append(';');
    return b.toString();
  }

  /**
   * Takes an existing {@link StringBuilder} and appends the string form of a {@link StructuredQuery} to it.
   * The structured query takes the form of:
   *
   * SELECT [{@link Column}][, Column]* FROM [{@link Queryable}][ WHERE [{@link Filter}]]?
   *
   * Example return for a simple query selecting columns CustomerName and Age from a table Customers where Age = 15:
   * SELECT CustomerName, Age FROM Customers WHERE (Age = 15)
   *
   * @param b The existing string builder
   * @param query The given query
   * @return The string builder with the query appended
   */
  private StringBuilder appendQuery(StringBuilder b, StructuredQuery query) {
    b.append("SELECT ");
    for (int i = 0; i < query.getColumns().size(); i++) {
      Column col = query.getColumns().get(i);
      appendColumn(b, col);
      if (col.hasAlias()) {
        appendAlias(b, col);
      }
      if (i < query.getColumns().size() - 1) {
        b.append(", ");
      }
    }
    b.append(" FROM ");
    appendQueryable(b, query.getQueryFrom());
    if (query.getFilter() != null) {
      b.append(" WHERE ");
      appendFilter(b, query.getFilter());
    }
    return b;
  }

  /**
   * Takes an existing {@link StringBuilder} and appends the string form of a {@link Column} to it. The column will be
   * prefixed with its From queryable's alias should it have one.
   * The column takes the form of:
   *
   * [fromAlias.]?[columnName]
   *
   * Example return for a column CustomerName from a table called Customers aliased as uscustomers:
   * uscustomers.CustomerName
   *
   * @param b The existing string builder
   * @param c The given column
   * @return The string builder with the column appended
   */
  private StringBuilder appendColumn(StringBuilder b, Column c) {
    if (c.hasFrom() && c.getFrom().hasAlias()) {
      b.append(c.getFrom().getAlias());
      b.append('.');
    }
    b.append(c.getName());
    return b;
  }

  /**
   * Takes an existing {@link StringBuilder} and appends the string form of a {@link Table} to it. Attempts to prepend
   * the project name and database names split by a period should they exist.
   * The table takes the form of:
   *
   * [projectName.]?[databaseName.]?[tableName]
   *
   * Example return with a table Customers with project testProject and a database global:
   * testProject.global.Customers
   *
   * @param b The existing string builder
   * @param t The given table
   * @return The string builder with the table appended
   */
  private StringBuilder appendTable(StringBuilder b, Table t) {
    b.append('`');
    if (t.hasProject()) {
      b.append(t.getProjectName());
      b.append('.');
    }
    if (t.hasDatabase()) {
      b.append(t.getDatabaseName());
      b.append('.');
    }
    b.append(t.getTableName());
    b.append('`');
    return b;
  }

  /**
   * Takes an existing {@link StringBuilder} and appends the string form of a {@link Queryable} to it. Attempts to
   * determine the queryable type before passing the queryable onto its respective helper function.
   * The queryable takes the form of:
   *
   * [({@link StructuredQuery})|{@link Table}][ AS aliasName]?
   *
   * Example return from a structured query selecting column CustomerName from a table Customers aliased as Temp:
   * (SELECT CustomerName FROM Customers) AS Temp
   *
   * Example return from a table Customers with project testProject and database global with alias uscustomers:
   * testProject.global.Customers AS uscustomers
   *
   *
   * @param b The existing string builder
   * @param q The given queryable
   * @return The string builder with the queryable appended
   */
  private StringBuilder appendQueryable(StringBuilder b, Queryable q) {
    QueryableType type = q.getType();
    if (type.equals(QueryableType.QUERY)) {
      StructuredQuery query = (StructuredQuery) q;
      b.append('(');
      appendQuery(b, query);
      b.append(')');
      if (query.hasAlias()) {
        appendAlias(b, query);
      }
      return b;
    } else if (type.equals(QueryableType.TABLE)) {
      Table table = (Table) q;
      appendTable(b, table);
      if (table.hasAlias()) {
        appendAlias(b, table);
      }
      return b;
    } else {
      throw new IllegalStateException("Unsupported queryable type " + q.getType());
    }
  }

  /**
   * Takes an existing {@link StringBuilder} and appends the string form of a {@link Filter} to it. Filters take the
   * form of a tree, so this appending is done through recursion.
   * The filter takes the form of:
   *
   * ([{@link Operand}|{@link Filter}] [{@link PredicateOperatorType}] [{@link Operand}|{@link Filter}])
   *
   * Both the left-hand side and the right-hand side must have the same type (Operand or Filter).
   *
   * Example return of a simple filter which checks whether Age is <= 15:
   * (Age <= 15)
   *
   * Example return of a nested filter which checks whether CustomerName = "John" or Age > 30:
   * ((CustomerName = "John") OR (Age > 30))
   *
   * @param b The existing string builder
   * @param f The given filter
   * @return The string builder with the filter appended
   */
  private StringBuilder appendFilter(StringBuilder b, Filter f) {
    b.append('(');
    if (f.isLeaf()) {
      appendOperand(b, f.getLeftOperand());
      b.append(' ');
      appendPredicateOperator(b, f.getOperation());
      b.append(' ');
      appendOperand(b, f.getRightOperand());
    } else {
      appendFilter(b, f.getLeftFilter());
      b.append(' ');
      appendPredicateOperator(b, f.getOperation());
      b.append(' ');
      appendFilter(b, f.getRightFilter());
    }
    b.append(')');
    return b;
  }

  /**
   * Takes an existing {@link StringBuilder} and appends the string form of an {@link Operand} to it. Operands must
   * be a {@link Column} or a {@link Constant}. This function simply checks the operand's type and passes it to its
   * respective helper function.
   *
   * @param b The existing string builder
   * @param o The given operand
   * @return The string builder with the operand appended
   */
  private StringBuilder appendOperand(StringBuilder b, Operand o) {
    OperandType type = o.getOperandType();
    if (type.equals(OperandType.COLUMN)) {
      return appendColumn(b, (Column) o);
    } else if (type.equals(OperandType.CONSTANT)) {
      return appendConstant(b, (Constant) o);
    } else {
      throw new IllegalStateException("Unsupported operand type " + o.getOperandType());
    }
  }

  /**
   * Takes an existing {@link StringBuilder} and appends the string form of a {@link PredicateOperatorType} to it.
   *
   * @param b The existing string builder
   * @param operation The given predicate operator type
   * @return The string builder with the predicate operator appended
   */
  private StringBuilder appendPredicateOperator(StringBuilder b, PredicateOperatorType operation) {
    switch(operation) {
      case OR:
        b.append("OR");
        break;
      case AND:
        b.append("AND");
        break;
      case LESS:
        b.append("<");
        break;
      case LESS_OR_EQUAL:
        b.append("<=");
        break;
      case EQUAL:
        b.append("=");
        break;
      case GREATER:
        b.append(">");
        break;
      case GREATER_OR_EQUAL:
        b.append(">=");
        break;
      default:
        throw new IllegalStateException("Unsupported predicate " + operation);
    }
    return b;
  }

  /**
   * Takes an existing {@link StringBuilder} and appends the string form of a {@link Constant} to it.
   * Constants may currently be of type {@link java.time.LocalDateTime}, {@link String}, or {@link Integer}.
   *
   * @param b The existing string builder
   * @param o The given constant object
   * @return The string builder with the constant appended
   */
  private StringBuilder appendConstant(StringBuilder b, Constant o) {
    switch (o.getConstantType()) {
      case DATETIME:
        // Convert the LocalDateTime to an Instant
        Instant i = ((DateTimeConstant) o).get().toInstant(ZoneOffset.UTC);
        // Convert to microsecond timestamp which is supported by BigQuery
        long timestamp = ChronoUnit.MICROS.between(Instant.EPOCH, i);
        b.append(timestamp);
        break;
      case INTEGER:
        b.append(((IntegerConstant) o).get());
        break;
      case STRING:
        b.append('"');
        b.append(((StringConstant) o).get());
        b.append('"');
        break;
      default:
        throw new IllegalStateException("Unsupported constant type " + o.getConstantType());
    }
    return b;
  }

  /**
   * Takes an existing {@link StringBuilder} and appends the alias of an {@link Aliasable} to it.
   * The alias takes the form of:
   *  AS [aliasName]
   *
   * Example return of a table Customers aliased as uscustomers:
   *  AS uscustomers
   *
   * @param b The existing string builder
   * @param a The given aliasable object
   * @return The string builder with the alias appended
   */
  private StringBuilder appendAlias(StringBuilder b, Aliasable a) {
    if (!a.hasAlias()) {
      throw new IllegalArgumentException("Aliasable object must have an alias");
    }
    b.append(" AS ");
    b.append(a.getAlias());
    return b;
  }
}

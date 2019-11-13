# GroupBy Aggregate


Description
-----------
Groups by one or more fields, then performs one or more aggregate functions on each group.
Supports `avg`, `count`, `count(*)`, `max`, `min`,`sum` as aggregate functions.

Use Case
--------
The transform is used when you want to create a group-by query in SQL.

Properties
----------
**Group By Fields:** List of fields to group by.
Records with the same value for all these fields will be grouped together.
Records output by this aggregator will contain all the group by fields and aggregate fields.
For example, if grouping by the ``user`` field and calculating an aggregate ``numActions:count(*)``,
output records will have a ``user`` field and a ``numActions`` field.

**Aggregates:** Aggregates to compute on each group of records.
Supported aggregate functions are `avg`, `count`, `count(*)`, `max`, `min`,`sum`.
An aggregate function must specify the field it is applied on, the function type, and the alias for the resulting field.

Example
-------
This example groups records by their ``user`` and ``item`` fields.
It then calculates two aggregates for each group. The first is a sum on ``price`` aliased as ``totalSpent``,
and the second counts the number of records in the group aliased as ``numPurchased``.

**Group By Fields:** ``user``, ``item``

**Aggregates:** ``SUM(price) AS totalSpent``, ``COUNT(*) AS numPurchased``

The following generalized SQL statement will be generated:

```
SELECT user, item, SUM(price) AS totalSpent, COUNT(*) as numPurchased FROM {input}
```

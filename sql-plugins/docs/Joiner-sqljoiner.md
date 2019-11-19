# Simple Joiner


Description
-----------
Represents a simple join operation on two inputs.

Use Case
--------
Useful for joining two tables on one or more conditions.


Properties
----------
**Join Keys:** List of keys to perform the join operation. Takes two fields and only supports equality.

**Join Type:** The type of join to perform. Supports INNER, LEFT, RIGHT, FULL, SEMI, and ANTI.


Example
-------
This example inner joins records from ``customers`` and ``purchases`` inputs on id and customer_id.

This will output the following generalized SQL:

```
SELECT * FROM customers INNER JOIN purchases ON id = customer_id;
```
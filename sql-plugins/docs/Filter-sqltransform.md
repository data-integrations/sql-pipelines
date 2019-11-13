# Filter SQL Transform


Description
-----------
The filter transform allows you to specify a simple filter with a left operand, right operand, and operator type.

Use Case
--------
The filter useful for filtering an SQL dataset.


Properties
----------
**Left Value:** Left operand value.

**Left Type:** Left operand type.

**Right Value:** Right operand value.

**Right Type:** Right operand type.

**Operation:** Operator type.


Example
-------
Suppose we want to filter for people with the name ``John``. We would use the following fields:

**Left Value:** ``Name``

**Left Type:** ``field``

**Right Value:** ``John``

**Right Type:** ``string``

**Operation:** ``equals``

This will output the following generalized SQL:

```
SELECT * FROM <input> WHERE Name = "John";
```
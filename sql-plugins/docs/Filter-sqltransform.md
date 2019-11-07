# Filter SQL Transform


Description
-----------
The filter transform allows you to specify a simple filter with a left operand, right operand, and operator type.

Use Case
--------
The filter useful for filtering an SQL dataset.


Properties
----------
**leftValue:** Left operand value.

**leftType:** Left operand type.

**rightValue:** Right operand value.

**rightType:** Right operand type.

**operation:** Operator type.


Example
-------

```json
{
    "name": "Filter",
    "type": "sqltransform",
    "properties": {
        "leftValue": "Name",
        "leftType": "field",
        "rightValue": "John",
        "rightType": "string",
        "operation": "equals"
    }
}
```

This will output the following generalized SQL:

```
SELECT * FROM <input> WHERE Name = "John";
```
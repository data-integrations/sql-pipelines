# Projection SQL Transform


Description
-----------
The Projection transform lets you select, rename, and cast fields to a different type.
Fields are first selected, then cast, then renamed.

Use Case
--------
The transform is used when you need to drop fields, keep specific fields, change field types, or rename fields.

For example, you may want to rename a field from ``'timestamp'`` to ``'ts'`` because you want
to write to a database where ``'timestamp'`` is a reserved keyword. You might want to
drop a field named ``'headers'`` because you know it is always empty for your particular
data source. Or, you might want to only keep fields named ``'ip'`` and ``'timestamp'`` and discard
all other fields.


Properties
----------
**select:** Comma-separated list of fields to select. For example: ``'field1,field2,field3'``.

**rename:** List of fields to rename. This is a comma-separated list of key-value pairs,
where each pair is separated by a colon and specifies the input and output names.

For example: ``'datestr:date,timestamp:ts'`` specifies that the ``'datestr'`` field should be
renamed to ``'date'`` and the ``'timestamp'`` field should be renamed to ``'ts'``.

**cast:** List of fields to convert to a different type. This is a comma-separated list
of key-value pairs, where each pair is separated by a colon and specifies the field name
and the desired type.

For example: ``'count:integer,price:double'`` specifies that the ``'count'`` field should be
converted to an integer and the ``'price'`` field should be converted to a double.

Only simple types are supported (boolean, int, long, float, double, bytes, string). Any
simple type can be converted to bytes or a string. Otherwise, a type can only be converted
to a larger type. For example, an int can be converted to a long, but a long cannot be
converted to an int.


Example
-------

```json
{
    "name": "Projection",
    "type": "sqlnode",
    "properties": {
        "cast": "cost:double",
        "rename": "cost:price",
            "select": "id,cost"
    }
}
```

This will output the following generalized SQL:

```
SELECT id, CAST(cost AS double) AS price FROM <input>;
```
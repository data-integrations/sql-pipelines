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
**Select:** List of fields to select.

**Rename:** List of fields to rename.

**Cast:** List of fields to convert to a different type.

Only simple types are supported (boolean, int, long, float, double, bytes, string). Any
simple type can be converted to bytes or a string. Otherwise, a type can only be converted
to a larger type. For example, an int can be converted to a long, but a long cannot be
converted to an int.


Example
-------
Say we want to convert the cost of a table to a double, rename the ``cost`` to ``price``, and select two fields, ``id``
and ``cost``.

**Select:** ``id``, ``cost``

**Rename:** Rename ``cost`` to ``price``

**Cast:** Cast ``cost`` to ``double``

This will output the following generalized SQL:

```
SELECT id, CAST(cost AS double) AS price FROM <input>;
```
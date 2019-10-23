# BigQuery Sink


Description
-----------
A node representing a BigQuery sink. Necessary for generating the final insert-into statement.

Use Case
--------
Necessary for generating the final insert-into statement for BigQuery.


Properties
----------
**project:** Project name.

**dataset:** Dataset name.

**table:** Table name.

**serviceAccountPath:** The path to the service account credentials file.


Example
-------

```json
{
    "name": "BigQueryTable",
    "type": "sqlsink",
    "properties": {
        "project": "a",
        "dataset": "b",
        "table": "c"
    }
}
```

This will output the following generalized SQL:

```
INSERT INTO `a.b.c` SELECT * FROM <input> ;
```
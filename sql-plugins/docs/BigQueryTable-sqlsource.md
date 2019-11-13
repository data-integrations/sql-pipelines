# BigQuery Source


Description
-----------
A node representing a BigQuery source. Necessary for generating the first select-from statement.

Use Case
--------
Necessary for generating the first select-from statement for BigQuery.


Properties
----------
**Project:** BigQuery project name.

**Dataset:** BigQuery dataset name.

**Table:** BigQuery table name.

**Service Account Path:** The path to the service account credentials file.

Example
-------

```json
{
    "name": "BigQueryTable",
    "type": "sqlsource",
    "properties": {
        "project": "a",
        "dataset": "b",
        "table": "c"
    }
}
```

This will output the following generalized SQL:

```
SELECT * FROM `a.b.c`;
```
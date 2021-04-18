# tjdbc

This is an extension for regular jdbc jar which provides implementation of standard temporal database operators.

### Setup

1. Include the jar in your project.
2. Create a statement proxy using

```
Statement statement = TJdbc.createStatement(connection);
```

3. Use this statement object for performing your queries

### Operators

#### 1. Temporalize

* Description \
  Provides a way to start temporal tracking of a table (All columns)
* Syntax
  ```
  temporalize table_name
  ```
* Example

  ```
  temporalize student
  ```

#### 2. First

* Description
* Syntax
* Example

#### 3. Last

* Description
* Syntax
* Example

#### 4. Tinsert

* Description
* Syntax
* Example

#### 5. Tupdate

* Description \
  Used for performing an update to a column/attribute of a record in a temporalized table
* Syntax
  ```
  tupdate table_name set column_name = <new_value> where id = <id>
  ```
* Example
  ```
  tupdate student set gpa = 8.2 where id = 1
  ```

#### 6. Next

* Description
* Syntax
* Example

#### 7. Previous

* Description
* Syntax
* Example
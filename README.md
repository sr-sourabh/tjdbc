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

* Description \
  Used for inserting values inside of a temporal table
* Syntax
  ```
  tinsert into table_name values ( value1,value2, .... )
  ```
* Example
  ```
  tinsert into student values (6,'Henry','active','3.75','CSE' )

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

#### 6. Tselect

* Description \
  Used to find an entity's attribute value on a particular date.
* Syntax
  ```
  tselect column_name from table_name where (specify primary key value) and date = 'yyyy-mm-dd'
  ```
* Example
  ```
  tselect gpa from student where id = 1 and date = '2019-01-21'

#### 7. Next

* Description
* Syntax
* Example

#### 8. Previous

* Description
* Syntax
* Example

#### 9. Coalesce

* Description \
  Used for combining i.e coalescing multiple entries that have same attribute values except for the start and end time.
* Syntax
  ```
  coalesce table_name
  ```
* Example
  ```
  coalesce president

#### 10. EvolutionFrom

* Description \
  Used to display the evolution dates along with values from val1 to current value
* Syntax
  ```
  EvolutionFrom table_name col val ;
  ```
* Example
  ```
  "EvolutionFrom student gpa 5.2 ;"
  "EvolutionFrom student gpa 5.2 where id = 1 ;"

#### 11. EvolutionFromAndTo

* Description \
  Used to display the evolution dates along with values from val1 to val2
* Syntax
  ```
  EvolutionFromAndTo table_name col val1 val2 ;
  ```
* Example
  ```
  "EvolutionFromAndTo student gpa 5.2 2.9 ;"
  "EvolutionFromAndTo student gpa 5.2 2.9 where id = 1 ;"

#### 12. Difference

* Description \
  Used to display the dates along with values excluding the intersection part
* Syntax
  ```
  tselect difference table1 e tjoin table2 d on e.d_id = d.d_id ;
  ```
* Example
  ```
  "tselect difference employee e tjoin department d on e.d_id = d.d_id ;"
  "tselect difference employee e tjoin department d on e.d_id = d.d_id where d.d_id = 1 ;"






# External Sort

Utility sorts binary files of 32-bit integers.

## Compile

To compile utility just run
```
mvn clean package
```
now you have `targer/sort.jar` file.

## Usage
Usage: sort filename [number of additional threads > 1];
Example: sort big_data 10

## Test
There are three predefined tests for files with 800000, 80000000 and 300000000 numbers.
To run them just run one of the following commands.

```
mvn test -Dtest=SortTest8000000
mvn test -Dtest=SortTest80000000
mvn test -Dtest=SortTest300000000
```

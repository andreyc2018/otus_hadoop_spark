# Решение к домашней работе "Знакомство с HDFS"

## Задача

Требуется написать Scala-приложение, которое будет очищать данные из папки `/stage` и складывать их в папку `/ods` в корне HDFS по следующим правилам:

1. Структура партиций (папок вида date=...) должна сохраниться.
1. Внутри папок должен остаться только один файл, содержащий все данные файлов из соответствующей партиции в папке `/stage`.

То есть, если у нас есть папка `/stage/date=2020-11-11` с файлами
``` text
part-0000.csv
part-0001.csv
```
то должна получиться папка `/ods/date=2020-11-11` с одним файлом
``` text
part-0000.csv
```
содержащим все данные из файлов папки `/stage/date=2020-11-11`.

## Решение

### Read, write files ignoring the CSV structure
1. Take input and output dirs as parameters
1. For each subdir in input dir
    1. Create similar dir in output dir 
    1. Open an output file of form `part-0000.csv`
    1. For each `*.csv` files in the input dir
    1. Read line by line and write to output file.

### Read and parse CSV files, merge and write
1. Take input and output dirs as parameters
1. For each subdir in input dir
    1. Create similar dir in output dir
    1. For each `*.csv` files in the input dir
    1. Read the files as `CSV` into DataFrame
    1. Merge the DataFrames
    1. Write resulting DataFrame to output dir 

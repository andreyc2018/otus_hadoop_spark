# HiveQL

## Домашнее задание
Напишете код для построения аналитической витрины в Hive
Цель: Практика с Hive на GCP DataProc

1. План домашней работы:
https://gist.github.com/kzzzr/f82e1511e8c38aa7d5c352a0ce308868

Что внутри:
- конфигурируем окружение
- создаем Metastore (MySQL), Dataproc (Хадуп-кластер)
- загружаем датасет (Chicago Taxi Trips)
- создаем таблицы, записываем данные в бинарный формат .parquet
- работаем с данными через SQL (Hive, Presto, Pyspark)

Команды запускаем в командной строке GCP по порядку. В коде даны пояснительные комментарии по каждой команде.

2. Вопросы для домашнего задания:

- Вывести динамику количества поездок помесячно
- Вывести топ-10 компаний (company) по выручке (trip_total)
- Подсчитать долю поездок <5, 5-15, 16-25, 26-100 миль

Ответы на эти вопросы (в виде выгрузки или скриншотов) нужно вложить в тред.
Для ответа на эти вопросы потребуется сформировать SQL-запрос и выполнить его (в Hive или Presto).

3. Альтернативно, если нет возможности работать в GCP, можно выполнить скрипты из обучающего материала HiveQL - SQL-доступ к данным - Apache Hive.
Результаты работы в виде 3-5 скриншотов вложить в качестве выполнения ДЗ.. 

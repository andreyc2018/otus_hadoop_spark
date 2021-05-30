# Разработка собственного коннектора на Spark

## Цель:
В данном ДЗ вы поработаете с DataSource API V2 с целью научиться писать свои собственные коннекторы для Spark. Задача - доработать data source для Postgres для партиционированного чтения.

* Склонируйте репозиторий https://github.com/Gorini4/spark_datasource_example
* Доработайте код в файле src/main/scala/org/example/datasource/postgres/PostgresDatasource.scala так, чтобы тест в файле src/test/scala/org/example/PostgresqlSpec.scala при выполнении читал таблицу users не в одну партицию, а в несколько (размер одной партиции должен задаваться через метод .option("partitionSize", "10")).

Рекомендация: Архитектуру решения можно обсудить в общей группе в Slack.

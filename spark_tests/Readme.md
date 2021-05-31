# Тестирование приложений Spark
Тесты для приложения Spark

## Цель:
Выполнив домашнее задание научитесь писать авто тесты для Spark jobs

## ДЗ предварительная инструкция
* Скачать и установить Idea Community - https://www.jetbrains.com/idea/download/#section=windows
* Установить плагин Скала
* Скачать и установить Java JDK 11 - https://www.oracle.com/java/technologies/javase-jdk11-downloads.html
* Скачать и установить git - https://git-scm.com/downloads
* Скачать и учтановить локально дистрибутив Hadoop (инструкция для Windows - https://www.datasciencecentral.com/profiles/blogs/how-to-install-and-run-hadoop-on-windows-for-beginners )
* Cкачать стартовый проект с Гитхаб c помощью команды git clone https://github.com/vadopolski/otus-hadoop-homework
* Запустить Idea и открыть скаченный проект File -> Open -> project folder/build.sbt
* Открыть в проекте файл src/main/scala/homework2/DataApiHomeWorkTaxi.scala запустить его, Ctrl + Shift10
* Скачать и установить docker-compose Из корневой папки проекта запустить сделать запуск - docker-compose up

## ДЗ основная инструкция и задания к занятию по Spark Testing
!!!!! ВНИМАНИЕ, для корректной работы необходимо сделать reimport dependency

### задание 1
Логически разбить на методы, написанный в домашнем задании к занятию Spark Data API. Пример src/main/scala/lesson2/OtusFragmentedByMethod.scala

#### Результат
В репозитории должен появиться код с описанием методов. Решение оформить в github.

### задание 2
Сформировать ожидаемый результат и покрыть простым тестом (с библиотекой AnyFlatSpec) витрину из домашнего задания к занятию Spark Data API, построенную с помощью RDD. Пример src/test/scala/lesson2/SimpleUnitTest.scala

#### Результат
В репозитории должен появиться код с тестом. Тест должен успешно выполняться при запуске. Решение оформить в github.

### задание 3
Сформировать ожидаемый результат и покрыть Spark тестом (с библиотекой SharedSparkSession) витрину из домашнего задания к занятию Spark Data API, построенную с помощью DF и DS. Пример src/test/scala/lesson2/TestSharedSparkSession.scala

#### Результат
В репозитории должен появиться код с тестом. Тест должен успешно выполняться при запуске. Решение оформить в github.

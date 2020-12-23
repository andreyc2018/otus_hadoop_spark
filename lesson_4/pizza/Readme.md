# Learning ScalaTests

## Run sbt with -deprecated
If `sbt compile` or `sbt test` prints an info message
```log
[warn] 3 deprecations (since 2.13.0); re-run with -deprecation for details
```
then run it as follows:
```sh
sbt '; set scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation") ; compile'
```
or
```sh
sbt '; set scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation") ; test'
```

## Links
* [User guide](https://www.scalatest.org/user_guide)
* [Selecting a test style](https://www.scalatest.org/user_guide/selecting_a_style)

ThisBuild / organization := "com.augustnagro"
ThisBuild / version := "2.0.0-SNAPSHOT"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-WunstableInlineAccessors")
ThisBuild / homepage := Some(uri("https://github.com/AugustNagro/magnum"))
ThisBuild / licenses += (
  "Apache-2.0",
  uri(
    "https://opensource.org/licenses/Apache-2.0"
  )
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    uri("https://github.com/AugustNagro/magnum"),
    "scm:git:git@github.com:augustnagro/magnum.git",
    Some("scm:git:git@github.com:augustnagro/magnum.git")
  )
)
ThisBuild / developers := List(
  Developer(
    id = "augustnagro@gmail.com",
    name = "August Nagro",
    email = "augustnagro@gmail.com",
    url = uri("https://augustnagro.com")
  )
)
ThisBuild / publishMavenStyle := true
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val centralSnapshots =
    "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}
ThisBuild / publish / skip := true

addCommandAlias("fmt", "scalafmtAll")

val testcontainersVersion = "0.44.1"
val circeVersion = "0.14.16"
val munitVersion = "1.3.6"
val postgresDriverVersion = "42.7.13"
val mssqlDriverVersion = "13.6.0.jre11"

lazy val root = project
  .in(file("."))
  .aggregate(magnum, magnumPg, magnumZio)

lazy val magnum = project
  .in(file("magnum"))
  .settings(
    publish / skip := false,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % munitVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "org.postgresql" % "postgresql" % postgresDriverVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-mysql" % testcontainersVersion % Test,
      "com.mysql" % "mysql-connector-j" % "26.7.0" % Test,
      "com.h2database" % "h2" % "2.5.250" % Test,
      "org.testcontainers" % "testcontainers-oracle-free" % "2.0.5" % Test,
      "com.oracle.database.jdbc" % "ojdbc17" % "23.26.3.0.0" % Test,
      "com.dimafeng" %% "testcontainers-scala-clickhouse" % testcontainersVersion % Test,
      ("com.clickhouse" % "clickhouse-jdbc" % "0.10.0" % Test)
        .classifier("all"),
      "org.xerial" % "sqlite-jdbc" % "3.53.4.0" % Test,
      "com.dimafeng" %% "testcontainers-scala-mssqlserver" % testcontainersVersion % Test,
      "com.microsoft.sqlserver" % "mssql-jdbc" % mssqlDriverVersion % Test
    )
  )

lazy val magnumPg = project
  .in(file("magnum-pg"))
  .dependsOn(magnum)
  .settings(
    publish / skip := false,
    libraryDependencies ++= Seq(
      "org.postgresql" % "postgresql" % postgresDriverVersion % "provided",
      "org.scalameta" %% "munit" % munitVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "io.circe" %% "circe-core" % circeVersion % Test,
      "io.circe" %% "circe-parser" % circeVersion % Test,
      "org.scala-lang.modules" %% "scala-xml" % "2.5.0" % Test
    )
  )

lazy val magnumZio = project
  .in(file("magnum-zio"))
  .dependsOn(magnum)
  .settings(
    publish / skip := false,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.1.26" % Provided,
      "org.scalameta" %% "munit" % munitVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "org.postgresql" % "postgresql" % postgresDriverVersion % Test
    )
  )

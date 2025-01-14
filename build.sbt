ThisBuild / organization := "com.augustnagro"
ThisBuild / version := "2.0.0-SNAPSHOT"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / scalaVersion := "3.3.4"
ThisBuild / scalacOptions ++= Seq("-deprecation")
ThisBuild / homepage := Some(url("https://github.com/AugustNagro/magnum"))
ThisBuild / licenses += (
  "Apache-2.0",
  url(
    "https://opensource.org/licenses/Apache-2.0"
  )
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/AugustNagro/magnum"),
    "scm:git:git@github.com:augustnagro/magnum.git",
    Some("scm:git:git@github.com:augustnagro/magnum.git")
  )
)
ThisBuild / developers := List(
  Developer(
    id = "augustnagro@gmail.com",
    name = "August Nagro",
    email = "augustnagro@gmail.com",
    url = url("https://augustnagro.com")
  )
)
ThisBuild / publishMavenStyle := true
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publish / skip := true

addCommandAlias("fmt", "scalafmtAll")

val testcontainersVersion = "0.41.4"
val circeVersion = "0.14.10"
val munitVersion = "1.1.0"
val postgresDriverVersion = "42.7.4"

lazy val root = project
  .in(file("."))
  .aggregate(magnum, magnumPg, magnumZio)

lazy val magnum = project
  .in(file("magnum"))
  .settings(
    Test / fork := true,
    publish / skip := false,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % munitVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "org.postgresql" % "postgresql" % postgresDriverVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-mysql" % testcontainersVersion % Test,
      "com.mysql" % "mysql-connector-j" % "9.0.0" % Test,
      "com.h2database" % "h2" % "2.3.232" % Test,
      "com.dimafeng" %% "testcontainers-scala-oracle-xe" % testcontainersVersion % Test,
      "com.oracle.database.jdbc" % "ojdbc11" % "21.9.0.0" % Test,
      "com.dimafeng" %% "testcontainers-scala-clickhouse" % testcontainersVersion % Test,
      "com.clickhouse" % "clickhouse-jdbc" % "0.6.0" % Test classifier "http",
      "org.xerial" % "sqlite-jdbc" % "3.46.1.3" % Test
    )
  )

lazy val magnumPg = project
  .in(file("magnum-pg"))
  .dependsOn(magnum)
  .settings(
    Test / fork := true,
    publish / skip := false,
    libraryDependencies ++= Seq(
      "org.postgresql" % "postgresql" % postgresDriverVersion % "provided",
      "org.scalameta" %% "munit" % munitVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "io.circe" %% "circe-core" % circeVersion % Test,
      "io.circe" %% "circe-parser" % circeVersion % Test,
      "org.scala-lang.modules" %% "scala-xml" % "2.3.0" % Test
    )
  )

lazy val magnumZio = project
  .in(file("magnum-zio"))
  .dependsOn(magnum)
  .settings(
    Test / fork := true,
    publish / skip := false,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.1.12" % Provided,
      "org.scalameta" %% "munit" % munitVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "org.postgresql" % "postgresql" % postgresDriverVersion % Test
    )
  )

lazy val magnumCats = project
  .in(file("magnum-cats-effect"))
  .dependsOn(magnum % "compile->compile;test->test")
  .settings(
    Test / fork := true,
    publish / skip := false,
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.5.7" % Provided,
      "org.tpolecat" %% "natchez-core" % "0.3.7" % Provided
    )
  )

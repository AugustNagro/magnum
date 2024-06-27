ThisBuild / organization := "com.augustnagro"
ThisBuild / version := "1.2.1"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / scalaVersion := "3.3.0"
ThisBuild / scalacOptions ++= Seq("-deprecation")
ThisBuild / homepage := Some(url("https://github.com/AugustNagro/magnum"))
ThisBuild / licenses += ("Apache-2.0", url(
  "https://opensource.org/licenses/Apache-2.0"
))
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
ThisBuild / credentials += Credentials(Path.userHome / ".sbt" / ".credentials")
ThisBuild / publish / skip := true

val testcontainersVersion = "0.40.12"

lazy val root = project
  .in(file("."))
  .aggregate(magnum, magnumPg)

lazy val magnum = project
  .in(file("magnum"))
  .settings(
    Test / fork := true,
    publish / skip := false,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "org.postgresql" % "postgresql" % "42.6.0" % Test,
      "com.dimafeng" %% "testcontainers-scala-mysql" % testcontainersVersion % Test,
      "mysql" % "mysql-connector-java" % "8.0.32" % Test,
      "com.h2database" % "h2" % "2.1.214" % Test,
      "com.dimafeng" %% "testcontainers-scala-oracle-xe" % testcontainersVersion % Test,
      "com.oracle.database.jdbc" % "ojdbc11" % "21.9.0.0" % Test,
      "com.dimafeng" %% "testcontainers-scala-clickhouse" % testcontainersVersion % Test,
      "com.clickhouse" % "clickhouse-jdbc" % "0.4.1" % Test classifier "http",
      "org.xerial" % "sqlite-jdbc" % "3.41.0.0" % Test
    )
  )

lazy val magnumPg = project
  .in(file("magnum-pg"))
  .dependsOn(magnum)
  .settings(
    Test / fork := true,
    publish / skip := false,
    libraryDependencies ++= Seq(
      "org.postgresql" % "postgresql" % "42.6.0" % "provided",
      "org.scalameta" %% "munit" % "0.7.29" % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test
    )
  )

inThisBuild(
  Seq(
    organization := "com.augustnagro",
    version := "0.1.0-SNAPSHOT",
    versionScheme := Some("early-semver"),
    scalaVersion := "3.3.0-RC3",
    scalacOptions ++= Seq("-deprecation"),
    homepage := Some(url("https://github.com/AugustNagro/magnum")),
    licenses += ("Apache-2.0", url(
      "https://opensource.org/licenses/Apache-2.0"
    )),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/AugustNagro/magnum"),
        "scm:git:git@github.com:augustnagro/magnum.git",
        Some("scm:git:git@github.com:augustnagro/magnum.git")
      )
    ),
    developers := List(
      Developer(
        id = "augustnagro@gmail.com",
        name = "August Nagro",
        email = "augustnagro@gmail.com",
        url = url("https://augustnagro.com")
      )
    )
  )
)

val mUnitVersion = "0.7.29"
val testcontainersVersion = "0.40.12"

lazy val magnum = project
  .in(file("."))
  .settings(
    Test / fork := true,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % mUnitVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-munit" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "org.postgresql" % "postgresql" % "42.5.4" % Test,
      "com.dimafeng" %% "testcontainers-scala-mysql" % testcontainersVersion % Test,
      "mysql" % "mysql-connector-java" % "8.0.32" % Test,
      "com.h2database" % "h2" % "2.1.214" % Test,
      "com.dimafeng" %% "testcontainers-scala-oracle-xe" % testcontainersVersion % Test,
      "com.oracle.database.jdbc" % "ojdbc11" % "21.9.0.0"
    )
  )

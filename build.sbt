inThisBuild(
  Seq(
    organization := "com.augustnagro",
    version := "0.1.0-SNAPSHOT",
    versionScheme := Some("early-semver"),
    scalaVersion := "3.3.0-RC3",
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

lazy val magnum = project
  .in(file("magnum"))
  .settings(
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % mUnitVersion % Test
    )
  )

lazy val magnumPgTests = project
  .in(file("magnum-pg-tests"))
  .dependsOn(magnum)
  .settings(
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % mUnitVersion % Test,
      "org.postgresql" % "postgresql" % "42.5.4"
    )
  )

lazy val magnumMySqlTests = project
  .in(file("magnum-mysql-tests"))
  .dependsOn(magnum)
  .settings(
    publish / skip := true,
    libraryDependencies ++= Seq(
      "mysql" % "mysql-connector-java" % "8.0.32",
      "org.scalameta" %% "munit" % mUnitVersion % Test
    )
  )

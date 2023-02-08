inThisBuild(
  Seq(
    organization := "com.augustnagro",
    version := "0.1.0-SNAPSHOT",
    versionScheme := Some("early-semver"),
    scalaVersion := "3.2.2",
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

lazy val magnumCore = project
  .in(file("magnum-core"))
  .settings(
    libraryDependencies += "org.scalameta" %% "munit" % mUnitVersion % Test
  )

lazy val magnumPg = project
  .in(file("magnum-pg"))
  .settings(
    libraryDependencies += "org.scalameta" %% "munit" % mUnitVersion % Test
  )

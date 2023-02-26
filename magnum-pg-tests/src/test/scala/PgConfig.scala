import scala.util.Properties.{propOrElse, propOrNone}

object PgConfig:
  object Db:
    val host = propOrElse("PgConfig.Db.host", "localhost")
    val name = propOrElse("PgConfig.Db.name", "test")
    val user =
      propOrNone("PgConfig.Db.user").orElse(propOrNone("user.name")).get
    val password = propOrElse("PgConfig.Db.password", "")
    val port = propOrElse("PgConfig.Db.port", "5432").toInt

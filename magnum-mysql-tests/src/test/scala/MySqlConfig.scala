import scala.util.Properties.{propOrElse, propOrNone}

object MySqlConfig:
  object Db:
    val host = propOrElse("MySqlConfig.Db.host", "localhost")
    val name = propOrElse("MySqlConfig.Db.name", "test")
    val user = propOrElse("MySqlConfig.Db.user", "root")
    val password = propOrElse("MySqlConfig.Db.password", "")
    val port = propOrElse("MySqlConfig.Db.port", "3306").toInt

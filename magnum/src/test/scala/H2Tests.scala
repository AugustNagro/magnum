import com.augustnagro.magnum.*
import munit.FunSuite
import org.h2.jdbcx.JdbcDataSource
import shared.*

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.util.Using
import scala.util.Using.Manager

class H2Tests extends FunSuite:

  sharedTests(this, H2DbType, xa)

  lazy val h2DbPath = Files.createTempDirectory(null).toAbsolutePath

  def xa(): Transactor =
    val ds = JdbcDataSource()
    ds.setURL("jdbc:h2:" + h2DbPath)
    ds.setUser("sa")
    ds.setPassword("")
    val tableDDLs = Vector(
      "/h2/car.sql",
      "/h2/person.sql",
      "/h2/my-user.sql",
      "/h2/no-id.sql",
      "/h2/big-dec.sql",
      "/h2/my-time.sql",
      "/h2/comp-id.sql"
    ).map(p =>
      Using.resource(getClass.getResourceAsStream(p))(stream =>
        String(stream.readAllBytes(), StandardCharsets.UTF_8)
      )
    )
    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      for ddl <- tableDDLs do stmt.execute(ddl)
    )
    Transactor(ds)
  end xa

end H2Tests

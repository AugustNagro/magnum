import com.augustnagro.magnum.*
import munit.FunSuite
import org.sqlite.SQLiteDataSource

import java.net.URI
import scala.compiletime.testing.typeCheckErrors

private object FieldCodecs:
  // The legacy schema stores each kind of numeric ID with its own text prefix.
  private def parsePrefixedId(prefix: String, stored: String): Long =
    require(stored.startsWith(prefix), s"Expected an ID starting with $prefix")
    stored.drop(prefix.length).toLong

  private def prefixedId(prefix: String): DbCodec[Long] =
    DbCodec[String].biMap(
      stored => parsePrefixedId(prefix, stored),
      id => s"$prefix$id"
    )

  val orderId: DbCodec[Long] = prefixedId("ord_")
  val customerId: DbCodec[Long] = prefixedId("cus_")
  val merchantId: DbCodec[Long] = prefixedId("mer_")
  val productId: DbCodec[Long] = prefixedId("prod_")
  val website: DbCodec[URI] =
    DbCodec[String].biMap(URI.create, _.toString)

@Table(SqliteDbType, SqlNameMapper.CamelToSnakeCase)
private case class OrderRecord(
    @Id @UsingDbCodec(FieldCodecs.orderId) orderId: Long,
    @UsingDbCodec(FieldCodecs.customerId) customerId: Long,
    @UsingDbCodec(FieldCodecs.merchantId) merchantId: Long
) derives DbCodec

@Table(SqliteDbType)
private case class MerchantWebsite(
    @Id id: Long,
    @UsingDbCodec(FieldCodecs.website) website: URI
) derives DbCodec

@Table(SqliteDbType, SqlNameMapper.CamelToSnakeCase)
private case class OrderLine(
    @Id @UsingDbCodec(FieldCodecs.orderId) orderId: Long,
    @Id @UsingDbCodec(FieldCodecs.productId) productId: Long,
    description: String
) derives DbCodec

class UsingDbCodecTests extends FunSuite:

  test("annotation supplies a codec without an implicit instance"):
    assertEquals(
      DbCodec[MerchantWebsite].cols.toSeq,
      Seq(java.sql.Types.BIGINT, java.sql.Types.VARCHAR)
    )

  test("annotation rejects a codec for the wrong field type"):
    val errors = typeCheckErrors("""
      import com.augustnagro.magnum.*
      @Table(SqliteDbType)
      case class Wrong(@UsingDbCodec(DbCodec[String]) value: Long) derives DbCodec
    """)
    assert(
      errors.exists(_.message.contains("requires DbCodec[")),
      errors.map(_.message).mkString("\n")
    )

  test("composite ID operations use each annotated field codec"):
    val ds = SQLiteDataSource()
    ds.setUrl("jdbc:sqlite::memory:")
    val repo = Repo[OrderLine, OrderLine, (Long, Long)]

    Transactor(ds).connect:
      sql"CREATE TABLE order_line (order_id TEXT, product_id TEXT, description TEXT, PRIMARY KEY (order_id, product_id))".update
        .run()

      val row = OrderLine(42L, 7L, "Coffee beans")
      repo.insert(row)
      assertEquals(
        sql"SELECT order_id, product_id FROM order_line"
          .query[(String, String)]
          .run(),
        Vector(("ord_42", "prod_7"))
      )
      assertEquals(repo.findById((42L, 7L)), Some(row))
      repo.deleteById((42L, 7L))
      assertEquals(repo.findById((42L, 7L)), None)

  test(
    "field codecs override the implicit codec in derived and repository operations"
  ):
    val ds = SQLiteDataSource()
    ds.setUrl("jdbc:sqlite::memory:")
    val repo = Repo[OrderRecord, OrderRecord, Long]

    Transactor(ds).connect:
      sql"CREATE TABLE order_record (order_id TEXT PRIMARY KEY, customer_id TEXT, merchant_id TEXT)".update
        .run()

      repo.insert(OrderRecord(42L, 17L, 23L))
      assertEquals(
        sql"SELECT order_id, customer_id, merchant_id FROM order_record"
          .query[(String, String, String)]
          .run(),
        Vector(("ord_42", "cus_17", "mer_23"))
      )
      assertEquals(
        sql"SELECT order_id, customer_id, merchant_id FROM order_record"
          .query[Option[OrderRecord]]
          .run(),
        Vector(Some(OrderRecord(42L, 17L, 23L)))
      )

      assertEquals(repo.findById(42L), Some(OrderRecord(42L, 17L, 23L)))
      repo.update(OrderRecord(42L, 18L, 24L))
      assertEquals(repo.findById(42L), Some(OrderRecord(42L, 18L, 24L)))

      assertEquals(
        sql"SELECT customer_id, merchant_id FROM order_record"
          .query[(String, String)]
          .run(),
        Vector(("cus_18", "mer_24"))
      )
end UsingDbCodecTests

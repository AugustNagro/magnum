import com.augustnagro.magnum.*
//import com.augustnagro.magnum.Util.sql

import javax.sql.DataSource

case class UsrCreator(first: String, addressId: Long)
case class Usr(id: Long, first: String, addressId: Long) derives DbReader

case class Address(id: Long, addr: String) derives DbReader

object Schema:
  val user = DbSchema[UsrCreator, Usr, Long]()
  val address = DbSchema[Address, Address, Long]()

val ds: DataSource = null

class UsrRepo extends Repo(ds, Schema.user):
  def getUserAddr(usrId: Long)(using DbCon): String =
    val user = Schema.user
    val address = Schema.address.alias("addr")
    sql"""
          select ${address.all}
          from $address
          join $user on ${user.addressId} = ${address.id}
          where ${user.id} = $usrId
       """.toString

object Yolo:
  def main(args: Array[String]): Unit =
    val repo = UsrRepo()
    println(repo.getUserAddr(4)(using null))

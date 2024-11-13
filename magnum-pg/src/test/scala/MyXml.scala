import com.augustnagro.magnum.DbCodec
import com.augustnagro.magnum.pg.xml.XmlDbCodec

import scala.xml.{Document, XML, Elem}

case class MyXml(elem: Elem)

object MyXml:
  given XmlDbCodec[MyXml] with
    def encode(a: MyXml): String = a.elem.toString
    def decode(xml: String): MyXml = MyXml(XML.loadString(xml))

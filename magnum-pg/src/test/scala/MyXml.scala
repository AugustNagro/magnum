import com.augustnagro.magnum.DbCodec
import com.augustnagro.magnum.pg.xml.XmlDbCodec

import scala.xml.{Document, XML, Elem}

case class MyXml(elem: Elem)

object MyXml:
  given DbCodec[MyXml] = new XmlDbCodec[MyXml]:
    def encode(a: MyXml): String = a.elem.toString
    def decode(xml: String): MyXml = MyXml(XML.loadString(xml))

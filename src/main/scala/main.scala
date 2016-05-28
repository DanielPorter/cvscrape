import java.io.File
import com.sksamuel.elastic4s.{ElasticsearchClientUri, ElasticClient}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType._
import play.api.libs.json.Json
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source._

case class Node(`match`: Double, name: String, id: String, playcount: Int=100, artist: String="")
object Node {
  implicit def format = Json.format[Node]
}
case class Link(source: String, target: String)
object Link {
  implicit def format = Json.format[Link]
}

case class d3Data(nodes: List[Node], links: List[Link])
object d3Data {
  implicit def format = Json.format[d3Data]
}

object main {
  def main(args: Array[String]): Unit = {
    val uri = ElasticsearchClientUri("elasticsearch://localhost:9300")
    val client = ElasticClient.transport(uri)
    val lines3 = fromInputStream(getClass.getResourceAsStream("/spn.txt")).getLines.filter(x=> x.nonEmpty)
    val lines2 = fromInputStream(getClass.getResourceAsStream("/alec institutions.txt")).getLines.filter(x=> x.nonEmpty)
    val lines = lines3.toList ++ lines2.toList

    val results: scala.collection.mutable.Map[String, scala.collection.mutable.ArrayBuffer[String]] = scala.collection.mutable.Map()
    val a =lines.toList.foreach { line =>
      val res = client.execute { search in "professors" / "cvs" query { matchPhraseQuery("text", line)} }.await
      res.hits.foreach { hit =>
        results.getOrElseUpdate(hit.sourceAsMap.get("name").get.asInstanceOf[String], scala.collection.mutable.ArrayBuffer()) += line
      }
    }
    println(results.toList.length)
    //results.toList.foreach { elem =>
    //println(elem._1, elem._2.mkString(", "))}

    val institutions = lines.map {inst => Node(1.0, inst, prettify(inst), playcount = 10000, artist="inst")}.toList
    println(institutions)
    val professors = results.keys.map{prof => Node(.5, prof, prettify(prof), playcount = 100000, artist="prof")}.toList
    val links = results.flatMap {prof => prof._2.map{inst => Link(prettify(prof._1), prettify(inst))}}.toList

    links.foreach { link =>
      val Pattern = """([a-zA-Z0-9])""".r
      link.source match {
        case Pattern(c) => println(c)
        case _ =>
      }
    }
    println(Json.toJson(d3Data(institutions ++ professors, links)))
  }

  def prettify(str: String): String = {
    str.replace(",", "").replace(" ", "").replace(".", "").replace("-", "").replace("(", "").replace(")", "")
  }
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.toList
    } else {
      List[File]()
    }
  }

  def uploadFiles(client: ElasticClient) = {
    val files = getListOfFiles("/home/daniel/Downloads/CV unkoch").filter(_.getName.endsWith(".txt"))
    files.foreach { f=>
      val text = fromFile(f).getLines().mkString("\n")
      client.execute { index into "professors" / "cvs" fields ("name"->f.getName, "text" ->text) }
    }
  }
}

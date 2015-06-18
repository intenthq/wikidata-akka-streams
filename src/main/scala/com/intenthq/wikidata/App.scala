package com.intenthq.wikidata

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorFlowMaterializer, ActorFlowMaterializerSettings}
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods._
import scopt.OptionParser

import scala.concurrent.{ExecutionContext, Future}
import scala.io.{Source => ioSource}
import scala.util.Try

object App {

  case class WikidataElement(id: String, sites: Map[String, String])

  case class Config(input: File = null, langs: Seq[String] = Seq.empty)

  def main(args: Array[String]) {
    sys.exit(execute(args, task))
  }

  def execute(args: Array[String], task: (Config => Int)): Int = {
    val parser = new OptionParser[Config]("wikidata") {
      opt[File]('i', "input") action { (i, c) =>
        c.copy(input = i) } required() text "Wikidata JSON dump"
      opt[Seq[String]]('l', "languages") action { (l, c) =>
        c.copy(langs = l) } required() valueName "<language1>,<language2>,..." text "Languages to take into account"
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        task(config)

      case None => 1
    }
  }

  def task(config: Config): Int = {
    implicit val system = ActorSystem("wikidata-process")
    val settings = ActorFlowMaterializerSettings(system).withInputBuffer(1, 1)
    implicit val materializer = ActorFlowMaterializer(Some(settings))

    import scala.concurrent.ExecutionContext.Implicits.global
    source(config.input)
      .via(parseJson(config.langs))
      .runWith(logEveryNSink(1000))
      .onComplete(x => system.shutdown())
    0
  }

  def source(file: File): Source[String, Unit] = {
    val compressed = new GZIPInputStream(new FileInputStream(file), 65536)
    val source = ioSource.fromInputStream(compressed, "utf-8")
    Source(() => source.getLines()).drop(1)
  }

  def parseJson(langs: Seq[String])(implicit ec: ExecutionContext): Flow[String, WikidataElement, Unit] =
    Flow[String].mapAsyncUnordered(8)(line => Future(parseItem(langs, line))).collect {
      case Some(v) => v
    }

  def parseItem(langs: Seq[String], line: String): Option[WikidataElement] = {
    Try(parse(line)).toOption.flatMap { json =>
      json \ "id" match {
        case JString(itemId) =>
          val sites = for {
            lang <- langs
            JString(title) <- json \ "sitelinks" \ s"${lang}wiki" \ "title"
          } yield lang -> title

          if (sites.isEmpty) None
          else Some(WikidataElement(id = itemId, sites = sites.toMap))

        case _ => None
      }
    }
  }

  def logEveryNSink[T](n: Int) = Sink.fold(0) { (x, y: T) =>
    if (x % n == 0)
      println(s"Processing element $x: $y")
    x + 1
  }

}

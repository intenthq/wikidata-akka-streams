package com.intenthq.wikidata

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.io.{Framing, InputStreamSource}
import akka.stream.scaladsl._
import akka.util.ByteString
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods._
import scopt.OptionParser

import scala.concurrent.{ExecutionContext, Future}
import scala.io.{Source => ioSource}
import scala.util.{Failure, Success, Try}

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
    implicit val system = ActorSystem("wikidata-poc")
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val elements = source(config.input).via(parseJson(config.langs))

    val g = FlowGraph.closed(count) { implicit b =>
      sinkCount => {
        import FlowGraph.Implicits._

        val broadcast = b.add(Broadcast[WikidataElement](2))
        elements ~> broadcast ~> logEveryNSink(1000)
                    broadcast ~> checkSameTitles(config.langs.toSet) ~> sinkCount
      }
    }

    g.run().onComplete { x =>
      x match {
        case Success((t, f)) => printResults(t, f)
        case Failure(tr) => println("Something went wrong")
      }
      system.shutdown()
    }
    0
  }

  def source(file: File): Source[String, Future[Long]] = {
    val compressed = new GZIPInputStream(new FileInputStream(file), 65536)
    InputStreamSource(() => compressed)
      .via(Framing.delimiter(ByteString("\n"), Int.MaxValue))
      .map(x => x.decodeString("utf-8"))
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

  def checkSameTitles(langs: Set[String]): Flow[WikidataElement, Boolean, Unit] = Flow[WikidataElement]
    .filter(_.sites.keySet == langs)
    .map { x =>
      val titles = x.sites.values
      titles.forall( _ == titles.head)
    }

  def count: Sink[Boolean, Future[(Int, Int)]] = Sink.fold((0,0)) {
    case ((t, f), true) => (t+1, f)
    case ((t, f), false) => (t, f+1)
  }

  def printResults(t: Int, f: Int) = {
    val message = s"""
                     | Number of items with the same title: $t
        | Number of items with the different title: $f
        | Ratios: ${t.toDouble / (t + f)} / ${f.toDouble / (t + f)}
                  """.stripMargin
    println(message)
  }

}

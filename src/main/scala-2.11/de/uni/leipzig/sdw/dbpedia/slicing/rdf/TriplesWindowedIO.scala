package de.uni.leipzig.sdw.dbpedia.slicing.rdf

import java.io.StringReader

import scala.collection.convert.decorateAll._
import com.hp.hpl.jena.graph.{Factory, Triple}
import grizzled.slf4j.Logging
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFLanguages}

import scala.language.postfixOps

/**
  * Created by Markus Ackermann.
  * No rights reserved.
  */
trait TriplesWindowedIO {

  def readTripleLines(lines: Iterator[String], langStr: Lang = RDFLanguages.NTRIPLES): Iterator[Triple]

  def serializeTripleLines(triples: Iterator[Triple]): Iterator[String]

  val READ_CHUNK_SIZE = 1000
  val SERIALISATION_CHUNK_SIZE = 1000
}

object RDFManagerTriplesIO extends TriplesWindowedIO with Serializable with Logging {

  override def readTripleLines(lines: Iterator[String], lang: Lang = RDFLanguages.NTRIPLES): Iterator[Triple] = {

    lines.grouped(READ_CHUNK_SIZE) map { tripleLines =>
      val reader = new StringReader(tripleLines.mkString("\n"))
      val graph = Factory.createDefaultGraph

      debug(s"reading lang: $lang")
      RDFDataMgr.read(graph, reader, "http://synthetic.base/", lang)

      graph.find(Triple.ANY).asScala.to[Iterable]
    } flatten
  }

  override def serializeTripleLines(triples: Iterator[Triple]): Iterator[String] = {
    triples.grouped(SERIALISATION_CHUNK_SIZE) map { tripleChunk =>
      val graph = Factory.createDefaultGraph
      tripleChunk foreach graph.add
      val outputStream = new ByteArrayOutputStream()
      RDFDataMgr.writeTriples(outputStream, tripleChunk.toIterator.asJava)
      outputStream.toString
    }
  }
}

object BananaTriplesIO extends TriplesWindowedIO with JenaBanana {

  override def readTripleLines(lines: Iterator[String], lang: Lang = RDFLanguages.NTRIPLES): Iterator[Triple] = {

    lines.grouped(READ_CHUNK_SIZE) map { tripleLines =>
      val reader = new StringReader(tripleLines.mkString("\n"))

      val graph = ntriplesReader.read(reader, "http://synthetic.base/").get
      ops.getTriples(graph)
    } flatten
  }

  override def serializeTripleLines(triples: Iterator[Triple]): Iterator[String] = {
    triples.grouped(SERIALISATION_CHUNK_SIZE) map { tripleChunk =>
      val graph = ops.makeEmptyMGraph()
      tripleChunk foreach graph.add

      val outputStream = new ByteArrayOutputStream()
      ntriplesWriter.write(graph, outputStream, "http://synthetic.base/")
      outputStream.toString
    }
  }
}
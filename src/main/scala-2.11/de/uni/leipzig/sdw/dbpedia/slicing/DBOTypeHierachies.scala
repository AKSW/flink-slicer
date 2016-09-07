package de.uni.leipzig.sdw.dbpedia.slicing

/**
  * Created by Markus Ackermann.
  * No rights reserved.
  */


import java.io.InputStream

import com.hp.hpl.jena.graph.{Factory, Graph}
import com.hp.hpl.jena.query.Query
import de.uni.leipzig.sdw.dbpedia.slicing.config.SliceConfig
import de.uni.leipzig.sdw.dbpedia.slicing.rdf.JenaBanana
import de.uni.leipzig.sdw.dbpedia.slicing.util.{DebugDurations, IRIStr}
import grizzled.slf4j.Logging
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.CompressorStreamFactory._
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}
import org.w3.banana.Prefix
import resource.ManagedResource

import scala.language.postfixOps
import scalax.io.{Resource => IOResource}


/**
  * Created by Markus Ackermann.
  * No rights reserved.
  */

trait DBpediaOntologyQuerying extends JenaBanana with DebugDurations with Logging {
  this: JenaBanana =>

  lazy val prefixes = Seq(
    Prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    Prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    Prefix("owl", "http://www.w3.org/2002/07/owl#"),
    Prefix("xsd", "http://www.w3.org/2001/XMLSchema#"),
    Prefix("dbp", "http://dbpedia.org/resource/"),
    Prefix("dbo", "http://dbpedia.org/ontology/")
  )

  def subClassesQuery(classURI: String) = qu(s"SELECT ?class { ?class rdfs:subClassOf* $classURI }")

  protected[sdw] def qu(quStr: String) = sparqlOps.parseSelect(quStr.stripMargin, prefixes).get
}

trait TypeHierarchies {

  def subClassIRIs(ancestor: IRIStr): Set[IRIStr] 
}


class DBOTypeHierachies(config: SliceConfig) extends TypeHierarchies with DBpediaOntologyQuerying {

  import sparqlGraph._
  import sparqlOps._

  protected[sdw] lazy val ontGraph: Rdf#Graph = {

    def parseStream(is: InputStream): Graph = debugDuration("reading ontology graph") {
      //      ntriplesReader.read(is, "http://ingnor.ed/").get
      val ontGraph = Factory.createDefaultGraph
      RDFDataMgr.read(ontGraph, is, RDFLanguages.TURTLE)
      ontGraph
    }

    def fromClassPath: ManagedResource[InputStream] = {
      def gzStream = getClass.getResourceAsStream("dbpedia_2015-10.nt.gz")
      def decompressionStream = new CompressorStreamFactory().createCompressorInputStream(GZIP, gzStream)
      IOResource.fromInputStream(decompressionStream)
    }

    def hasCompressionExtension = config.externalDBpediaOntologyPath.fold(false) { path =>
      val compressedRegex = """\.(gz)|(gzip)|(bz2)|(bzip2)$""".r

      compressedRegex.findFirstIn(path.segments.last).isDefined
    }

    def ontStream = config.externalDBpediaOntologyPath.fold[ManagedResource[InputStream]](fromClassPath) { path =>

      path.inputStream().map { is =>
        if (hasCompressionExtension) {
          new CompressorStreamFactory().createCompressorInputStream(is)
        } else is
      }
    }

    ontStream.acquireAndGet(parseStream)
  }

  protected[sdw] def classSolutions(qu: Query): Set[IRIStr] = {
    executeSelect(ontGraph, qu, Map.empty) map { result =>
      solutionIterator(result).map(_.getResource("class").toString).toSet
    } get
  }

  override def subClassIRIs(ancestor: IRIStr): Set[IRIStr] = classSolutions(subClassesQuery(ancestor))
}



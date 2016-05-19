package de.uni.leipzig.sdw.dbpedia.slicing.util

import java.io.File
import java.lang.Iterable
import java.nio.file.{Path, Paths}

import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import de.uni.leipzig.sdw.dbpedia.slicing.config.SliceConfig
import de.uni.leipzig.sdw.dbpedia.slicing.rdf.{RDFManagerTriplesIO, TriplesWindowedIO}
import org.apache.flink.api.common.functions.MapPartitionFunction
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import org.apache.jena.riot.RDFLanguages

import scala.collection.convert.decorateAll._
import scala.collection.convert.wrapAll._
import scala.language.postfixOps


/**
  * Created by Markus Ackermann.
  * No rights reserved.
  */

object SliceOps {

  def readTripleDataset(filePath: String, env: ExecutionEnvironment, tio: TriplesWindowedIO) = {

    val basename = Paths.get(filePath).getFileName

    val jenaLangStr = RDFLanguages.filenameToLang(basename.toString, RDFLanguages.NTRIPLES).getName

    val mapFunction = new MapPartitionFunction[String, Triple] {

      override def mapPartition(values: Iterable[String], out: Collector[Triple]): Unit = {
        tio.readTripleLines(values.toIterator, RDFLanguages.nameToLang(jenaLangStr)).foreach(out.collect)
      }
    }

    env.readTextFile(filePath).mapPartition(mapFunction)
  }

  def filteredInstances(instanceTypes: DataSet[Triple], subClassIRIs: Set[IRIStr]) = instanceTypes.filter { triple =>
    (subClassIRIs contains triple.getObject.getURI) && (triple.getPredicate.getURI == typePropertyStr)
  } map { _.getSubject.getURI } distinct

  def writeTripleDataset(tds: DataSet[Triple], filePath: String, tio: TriplesWindowedIO,
                         writeMode: WriteMode = WriteMode.OVERWRITE,
                         parallelism: Int = 1) = {

    val sink = tds.mapPartition(tio.serializeTripleLines _).writeAsText(filePath, writeMode)

    if(parallelism > 0) {
      sink.setParallelism(parallelism)
    }

    sink
  }

  protected val typePropertyStr = RDF.`type`.getURI
}

class SliceOps(env: ExecutionEnvironment, config: SliceConfig, tio: TriplesWindowedIO) {

  import SliceOps._

  lazy val combinedFacts: DataSet[Triple] =
    readTripleDataset(dumpFilePathStr(config.combinedStatementsFilename), env, tio)

  lazy val instanceTypes: DataSet[Triple] =
    readTripleDataset(dumpFilePathStr(config.instanceTypesFilename), env, tio)

  def selectViaSubClasses(subClassIRIs: Set[IRIStr]): IRIDataset =
    SliceOps.filteredInstances(instanceTypes, subClassIRIs)

  def factsForSubjects(subjects: IRIDataset): DataSet[Triple] = {
    val join = subjects.joinWithHuge(combinedFacts).where(identity(_)).equalTo { _.getSubject.getURI }

    join apply { (_, triple) => triple }
  }

  def mentionedEntitiesFacts(statements: DataSet[Triple], mentionCandidates: IRIDataset): DataSet[Triple] = {
    val join = statements.filter(_.getObject.isURI) join mentionCandidates where { _.getObject.getURI } equalTo { identity(_) }


    val mentionedInstances = join apply { (_, subjUri) => subjUri } distinct

    factsForSubjects(mentionedInstances)
  }

  def filterOnObjectPosition(statements: DataSet[Triple], mentionCandidates: IRIDataset): DataSet[Triple] = {
    val join = statements.filter(_.getObject.isURI) join mentionCandidates where { _.getObject.getURI } equalTo { identity(_) }


    join apply { (triple, _) => triple } distinct {
      t => (t.getSubject.getURI, t.getPredicate.getURI, t.getObject.toString(true))
    }
  }

  def writeTripleDataset(tds: DataSet[Triple], filePath: String,
                         writeMode: WriteMode = WriteMode.OVERWRITE,
                         parallelism: Int = 1) = SliceOps.writeTripleDataset(tds, filePath, tio, writeMode, parallelism)


  protected def dumpFilePathStr(fileName: String) =
    s"${config.dbpediaDumpDir}${File.separator}$fileName"

  def sinkPathStr(contentDesc: String, distDesc: String = config.distributionDescriptionInfix,
                            ext:String = "nt") =
    s"${config.sliceDestinationDir}${File.separator}$contentDesc$distDesc.$ext"
}

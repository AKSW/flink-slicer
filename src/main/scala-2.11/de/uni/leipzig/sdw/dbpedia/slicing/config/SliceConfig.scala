package de.uni.leipzig.sdw.dbpedia.slicing.config

import java.nio.file.{FileSystems, Files}

import scalax.file.Path
import de.uni.leipzig.sdw.dbpedia.slicing.util._

import org.apache.flink.configuration.Configuration
import org.apache.jena.iri.{IRI, IRIFactory}

import scalax.file.defaultfs.DefaultPath

/**
  * Created by Markus Ackermann.
  * No rights reserved.
  */

object SliceConfig {

  val CombinedStatementsFilenameDefault = "combined.ttl"
  val InstanceTypesFilenameDefault = "instance_types_en.ttl"
  val DistributionDescriptionInfixDefault = ""
  val ExternalDBpediaOntologyPathDefault = None

  val DefaultPropertiesFilename = "dbp-slicing.properties"
}



trait SliceConfig {

  import SliceConfig._

  /** place to look for uncompressed dbpedia NTriples data to load and slice*/
  def dbpediaDumpDir: Path

  /** output destination for slice result files (NTriples)*/
  def sliceDestinationDir: DefaultPath

  /** filename of the input NTriples file containing the concatenation of all DBpedia dump files the slice
    * should be cut from */
  val combinedStatementsFilename: String = CombinedStatementsFilenameDefault

  def combinedStatementsFile = dbpediaDumpDir / combinedStatementsFilename

  /** filename of the input NTriples file containing mapping-based ontology types for DBpedia resources */
  val instanceTypesFilename: String = InstanceTypesFilenameDefault

  def instanceTypesFile = dbpediaDumpDir / instanceTypesFilename

  /** a filename infix to distinguish slice results, e.g. by dbpedia language and URI scheme (_de_en-uris) */
  val distributionDescriptionInfix = DistributionDescriptionInfixDefault

  /** if defined, this path is used to load the DBpedia ontology to query for type hierarchies instead of using
    * the bundled DBO file (version 2015/10) */
  val externalDBpediaOntologyPath: Option[Path] = ExternalDBpediaOntologyPathDefault

  def subClassListPath(iri: IRIStr) = {

    val cleanBaseName = IRIFactory.iriImplementation().construct(iri).toASCIIString.
      replaceAllLiterally(FileSystems.getDefault.getSeparator, "~")

    subClassListDir / s"DBO-subClasses-$cleanBaseName"
  }

  lazy val subClassListDir: Path = {
    val dir = sliceDestinationDir / "subclassLists"
    Files.createDirectories(dir.jfile.toPath)
    dir
  }
}

object PropertiesFileSliceConfig {

  def apply(config: Configuration): PropertiesFileSliceConfig = new PropertiesFileSliceConfig(config)

  val DBpediaDumpDirKey = "dbpedia.dump.dir"
  val SliceDestinationDirKey = "slice.destination.dir"
  val CombinedStatementsFilenameKey = "combined.statements.filename"
  val InstanceTypesFilenameKey = "instance.types.filename"
  val DistributionDescriptionInfixKey = "distribution.description.infix"
  val ExternalDBpediaOntologyPathKey = "external.ontology.path"
}

class PropertiesFileSliceConfig(config: Configuration) extends SliceConfig {
  import SliceConfig._
  import PropertiesFileSliceConfig._

  override val dbpediaDumpDir = pathFromRequiredString(DBpediaDumpDirKey)

  /** output destination for slice result files (NTriples) */
  override val sliceDestinationDir = pathFromRequiredString(SliceDestinationDirKey)

  override val externalDBpediaOntologyPath =
    Option(config.getString(ExternalDBpediaOntologyPathKey, null)).map(Path.fromString)

  override val combinedStatementsFilename =
    config.getString(CombinedStatementsFilenameKey, CombinedStatementsFilenameDefault)

  override val instanceTypesFilename =
    config.getString(InstanceTypesFilenameKey, InstanceTypesFilenameDefault)

  override val distributionDescriptionInfix =
    config.getString(DistributionDescriptionInfixKey, DistributionDescriptionInfixDefault)

  protected def pathFromRequiredString(key: String) = {
    Option(config.getString(key, null)).fold(sys.error(s"$key is required"))(Path.fromString)
  }
}

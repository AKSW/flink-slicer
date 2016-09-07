package de.uni.leipzig.sdw.dbpedia.slicing

import de.uni.leipzig.sdw.dbpedia.slicing.rdf.RDFManagerTriplesIO
import de.uni.leipzig.sdw.dbpedia.slicing.util.{SliceOps, _}
import grizzled.slf4j.Logging
import org.apache.flink.api.scala._

import scala.language.postfixOps

object CompanyFacts extends Logging {

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val jobName: String = "company facts"
    val sliceConfig = sliceConfigFromCliArgs(args)
    val so = new SliceOps(env, sliceConfig, RDFManagerTriplesIO)
    val th = new DBOTypeHierachies(sliceConfig)

    lazy val companyInstances = so.selectViaSubClasses(th.subClassIRIs("dbo:Company"))
    val companyFacts = so.factsForSubjects(companyInstances)
    so.writeTripleDataset(companyFacts, so.sinkPath("company-facts"))

    env.execute(jobName)
  }
}

object CombinedSlice extends Logging {


  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val jobName = "combined-slice"
    val sliceConfig = sliceConfigFromCliArgs(args)
    val th = new DBOTypeHierachies(sliceConfig)
    val so = new SliceOps(env, sliceConfig, RDFManagerTriplesIO)

    val companySubclasses = th.subClassIRIs("dbo:Company")
    val placeSubclasses = th.subClassIRIs("dbo:Place")

    info(s"${companySubclasses.size} subclasses for companies:\n" + companySubclasses.mkString("\n"))
    info(s"${placeSubclasses.size} subclasses for places:\n" + placeSubclasses.mkString("\n"))

    //new PlaceFacts(env, th, so).addToProgram
//    new CombinedSlice(env, th, so).addToProgramm
    new GeneralisedSlice(env, th, so).addToProgram

    info("Starting Flink program")
    env.execute(jobName)
    info("Flink program finshed")
  }
}

class CombinedSlice(env: ExecutionEnvironment, th: TypeHierarchies, so: SliceOps) {

  lazy val companyInstances = so.selectViaSubClasses(th.subClassIRIs("dbo:Company"))

  lazy val eventInstances = so.selectViaSubClasses(th.subClassIRIs("dbo:Event"))

  lazy val personInstances = so.selectViaSubClasses(th.subClassIRIs("dbo:Person"))

  lazy val placeInstances = so.selectViaSubClasses(th.subClassIRIs("dbo:Place"))

  lazy val addToProgramm = {
    val companyFacts = so.factsForSubjects(companyInstances)
    so.writeTripleDataset(companyFacts, so.sinkPath("company-facts"))

    val companyEventFacts = so.mentionedEntitiesFacts(companyFacts, eventInstances)
    so.writeTripleDataset(companyEventFacts, so.sinkPath("company-event-facts"))

    val companyPersonFacts = so.mentionedEntitiesFacts(companyFacts, personInstances)
    so.writeTripleDataset(companyPersonFacts, so.sinkPath("company-person-facts"))

    val companyPlaceFacts = so.mentionedEntitiesFacts(companyFacts, placeInstances)
    so.writeTripleDataset(companyPlaceFacts, so.sinkPath("company-place-facts"))

    val companyAsObjectFacts = so.filterOnObjectPosition(so.combinedFacts, companyInstances)
    so.writeTripleDataset(companyAsObjectFacts, so.sinkPath("company-in-obj-pos"))
  }
}

class PlaceFacts(env: ExecutionEnvironment, th: TypeHierarchies, so: SliceOps) {

  lazy val placeInstances = so.selectViaSubClasses(th.subClassIRIs("dbo:Place"))

  lazy val placeFacts = so.factsForSubjects(placeInstances)

  lazy val addToProgram = {
    so.writeTripleDataset(placeFacts, so.sinkPath("place-facts"))
  }
}

class GeneralisedSlice(env: ExecutionEnvironment, th: TypeHierarchies, so: SliceOps) {

  lazy val companyInstances = so.selectViaSubClasses(th.subClassIRIs("dbo:Company"))

  lazy val organisationInstances = so.selectViaSubClasses(th.subClassIRIs("dbo:Organisation"))

  lazy val addToProgram = {

    val starts = companyInstances.union(organisationInstances).distinct

    val companyOrgaFacts = so.factsForSubjects(starts).distinct(_.toString)
    so.writeTripleDataset(companyOrgaFacts, so.sinkPath("company-organisation-facts"))

    val referencedEntitiesFacts = so.mentionedEntitiesFacts(companyOrgaFacts).distinct(_.toString)
    so.writeTripleDataset(referencedEntitiesFacts, so.sinkPath("referenced-entities-facts"))

    val companyOrgaAsObjectFacts = so.filterOnObjectPosition(so.combinedFacts, companyInstances).distinct(_.toString)
    so.writeTripleDataset(companyOrgaAsObjectFacts, so.sinkPath("company-organisation-in-obj-pos"))
  }
}

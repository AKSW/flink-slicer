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

    lazy val companyInstances = so.selectViaSubClasses(th.companySubClassIRIs)
    val companyFacts = so.factsForSubjects(companyInstances)
    so.writeTripleDataset(companyFacts, so.sinkPathStr("company-facts"))

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

    info(s"${th.companySubClassIRIs.size} subclasses for companies:\n" + th.companySubClassIRIs.mkString("\n"))
    info(s"${th.placeSubClassIRIs.size} subclasses for companies:\n" + th.placeSubClassIRIs.mkString("\n"))




    new CombinedSlice(env, th, so).addToProgramm

    info("starting Flink program")
    env.execute(jobName)
    info("Flink program finshed")
  }
}

class CombinedSlice(env: ExecutionEnvironment, th: TypeHierarchies, so: SliceOps) {

  lazy val companyInstances = so.selectViaSubClasses(th.companySubClassIRIs)

  lazy val eventInstances = so.selectViaSubClasses(th.eventSubClassIRIs)

  lazy val personInstances = so.selectViaSubClasses(th.personSubClasseIRIs)

  lazy val placeInstances = so.selectViaSubClasses(th.placeSubClassIRIs)

  lazy val addToProgramm = {
    val companyFacts = so.factsForSubjects(companyInstances)
    so.writeTripleDataset(companyFacts, so.sinkPathStr("company-facts"))

    val companyEventFacts = so.mentionedEntitiesFacts(companyFacts, eventInstances)
    so.writeTripleDataset(companyEventFacts, so.sinkPathStr("company-event-facts"))

    val companyPersonFacts = so.mentionedEntitiesFacts(companyFacts, personInstances)
    so.writeTripleDataset(companyPersonFacts, so.sinkPathStr("company-person-facts"))

    val placeFacts = so.mentionedEntitiesFacts(companyFacts, placeInstances)
    so.writeTripleDataset(placeFacts, so.sinkPathStr("company-place-facts"))

    val companyAsObjectFacts = so.filterOnObjectPosition(so.combinedFacts, companyInstances)
    so.writeTripleDataset(companyAsObjectFacts, so.sinkPathStr("company-in-obj-pos"))

    ()
  }
}

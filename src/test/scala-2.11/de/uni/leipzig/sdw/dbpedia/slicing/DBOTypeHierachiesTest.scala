package de.uni.leipzig.sdw.dbpedia.slicing

import java.io.File
import java.nio.file.Path

import de.uni.leipzig.sdw.dbpedia.slicing.config.SliceConfig
import org.scalatest.{FlatSpec, FunSuite, Matchers}

/**
  * Created by Markus Ackermann.
  * No rights reserved.
  */
class DBOTypeHierachiesTest extends FlatSpec with Matchers{

  val configStub = new SliceConfig {
    override def sliceDestinationDir: File = ???

    override def dbpediaDumpDir: Path = ???
  }

  new DBOTypeHierachies(configStub) {


    "SPARQL query components for the DBpedia ontology" should
      "retrieve some sub-classes for dbo:Company, dbo:Event, dbo:Person, dbo:Place" in {

      Set(companySubClassIRIs, eventSubClassIRIs, personSubClasseIRIs, personSubClasseIRIs) foreach {
        _ should not be empty
      }
    }
  }
}

package de.uni.leipzig.sdw.dbpedia.slicing

import java.io.{Closeable, File}
import java.nio.file.{Files, Path, Paths}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.google.common.base.Stopwatch
import de.uni.leipzig.sdw.dbpedia.slicing.config.{PropertiesFileSliceConfig, SliceConfig}
import grizzled.slf4j.Logging
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.ConfigConstants._
import org.apache.flink.configuration.Configuration
import org.apache.jena.riot.{Lang, RDFLanguages}

import scala.language.implicitConversions

/**
  * Created by Markus Ackermann.
  * No rights reserved.
  */
package object util {

  type IRIStr = String
  type IRIDataset = DataSet[IRIStr]

  val INSTANCE_TYPES_EN = "/opt/datasets/dbpedia/2015-03/instance-types_en.nt"
  val COMBINED_EN = "/opt/datasets/dbpedia/2015-03/combined.nt"
  val INSTANCE_TYPES_DE_LOC = "/opt/datasets/dbpedia/2015-04-de-loc/instance-types_de.nt"

  implicit class ClosableOps[C <: AutoCloseable](val self: C) {

    def loanTo[T](operation: C => T) = try {
      operation(self)
    } finally {
      self.close()
    }
  }

  implicit def javaFileToJavaPath(file: File): Path = file.toPath

  implicit def javaPathToJavaFile(path: Path): File = path.toFile

  def sliceConfigFromPropertiesFile(configPath: String) = {
    val pt = ParameterTool.fromPropertiesFile(configPath)
    PropertiesFileSliceConfig(pt.getConfiguration)
  }

  def sliceConfigFromCliArgs(args: Array[String]) = {
    args.headOption.fold({
      val defaultPath = Paths.get(SliceConfig.DefaultPropertiesFilename)
      if(Files.isRegularFile(defaultPath)) {
        sliceConfigFromPropertiesFile(defaultPath.toString)
      } else {
        sys.error("slice config properties file unspecified and fallback " +
          s"'${defaultPath.toAbsolutePath.normalize}' does not exist")
      }
    }) { configPath =>
      sliceConfigFromPropertiesFile(configPath)
    }
  }

  def createLocalExecutionEnvironmentForAKSWNC7 = {
    val config = new Configuration()
    config.setInteger(DEFAULT_PARALLELISM_KEY, 48)
    config.setInteger(TASK_MANAGER_NUM_TASK_SLOTS, 48)
    config.setString(TASK_MANAGER_TMP_DIR_KEY, "/data/tmp/flink1:/data/tmp/flink2")
    config.setString(BLOB_STORAGE_DIRECTORY_KEY, "/data/tmp/flink-blobs")
    config.setInteger(TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 131072)

    val env = ExecutionEnvironment.createLocalEnvironment(config)
    env.addDefaultKryoSerializer(classOf[Path], new Serializer[Path] with Serializable {

      override def read(kryo: Kryo, input: Input, `type`: Class[Path]): Path = {
        Paths.get(input.readString())
      }

      override def write(kryo: Kryo, output: Output, path: Path): Unit = {
        output.writeString(path.toAbsolutePath.normalize().toString)
      }
    })

    env
  }

  trait DebugDurations { this: Logging =>

    def debugDuration[T](workDescription: String)(work: => T) = {
      val sw = Stopwatch.createStarted()
      val res = work
      sw.stop()
      debug(s"$workDescription took $sw")
      res
    }
  }
}

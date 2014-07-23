/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.cli

import org.apache.hadoop.mapreduce.Job
import org.bdgenomics.adam.util.ParquetLogger
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.rdd.RDD
import java.util.logging.Level

object WebUi extends ADAMCommandCompanion {
  val commandName = "webui"
  val commandDescription = "Oink oink moo moo"

  def apply(cmdLine: Array[String]) = {
    new WebUi(Args4j[WebUiArgs](cmdLine))
  }
}

class WebUiArgs extends Args4jBase with ParquetArgs with SparkArgs {
}

class WebUi(protected val args: WebUiArgs) extends ADAMSparkCommand[WebUiArgs] with Logging {
  val companion = WebUi

  def run(sc: SparkContext, job: Job) {

    // Quiet Parquet...
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val server = new org.eclipse.jetty.server.Server(8080)
    val handlers = new org.eclipse.jetty.server.handler.ContextHandlerCollection()
    server.setHandler(handlers)
    handlers.addHandler(new org.eclipse.jetty.webapp.WebAppContext("adam-cli/src/main/web", "/"))
    server.start
    server.join
  }

}

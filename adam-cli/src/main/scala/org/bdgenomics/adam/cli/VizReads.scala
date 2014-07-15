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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferenceRegion, TrackedLayout }
import org.bdgenomics.adam.projections.ADAMRecordField._
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.ADAMRecord
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

import scala.math._

import org.scalatra.ScalatraServlet
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.fusesource.scalate.TemplateEngine

object VizReads extends ADAMCommandCompanion {
  val commandName: String = "viz"
  val commandDescription: String = "Generates images from sections of the genome"

  var refName: String = null
  var reads: RDD[ADAMRecord] = null

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new VizReads(Args4j[VizReadsArgs](cmdLine))
  }

  def draw(region: ReferenceRegion, layout: TrackedLayout): JValue = {
    val height = 400
    val width = 400

    val base = 10
    val trackHeight = min(20, (height - base) / (layout.numTracks + 1))
    var tracks = new scala.collection.mutable.ListBuffer[Track]

    // draws a box for each read, in the appropriate track.
    for ((rec, track) <- layout.trackAssignments) {

      val ry1 = height - base - trackHeight * (track + 1)

      val rxf = (rec.getStart - region.start).toDouble / region.width.toDouble
      val rx1: Int = round(rxf * width).toInt
      val rxwf = rec.referenceLength.toDouble / region.width.toDouble
      val rw: Int = max(round(rxwf * width) - 1, 1).toInt // at least make it one-pixel wide.

      tracks += new Track(rec.getReadName, rx1, ry1, rw, trackHeight)
    }
    val trackList = tracks.toList

    "trackInfo" ->
      trackList.map { t =>
        ("readName" -> t.readName) ~
          ("x" -> t.x) ~
          ("y" -> t.y) ~
          ("w" -> t.w) ~
          ("h" -> t.h)
      }
  }
}

case class Track(readName: String, x: Long, y: Long, w: Long, h: Long)

class VizReadsArgs extends Args4jBase with SparkArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM Records file to view", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "REFNAME", usage = "The reference to view", index = 1)
  var refName: String = null
}

class VizServlet extends ScalatraServlet {
  var regInfo = new ReferenceRegion("Chromosome", 0L, 100L)

  get("/?") {
    redirect(url("reads"))
  }

  get("/reads/?") {
    post(url("/reads/", Iterable("ref" -> regInfo.referenceName, "start" -> regInfo.start, "end" -> regInfo.end)))

    val templateEngine = new TemplateEngine
    templateEngine.layout("adam-cli/src/main/webapp/WEB-INF/layouts/default.ssp", Map("regInfo" -> regInfo))
  }

  post("/reads/:ref") {
    contentType = "text/html"

    regInfo = new ReferenceRegion(params("ref"), params("start").toLong, params("end").toLong)
    val region = new ReferenceRegion(regInfo.referenceName, regInfo.start, regInfo.end)
    VizReads.draw(region, new TrackedLayout(VizReads.reads.filterByOverlappingRegion(region).collect()))
  }
}

class VizReads(protected val args: VizReadsArgs) extends ADAMSparkCommand[VizReadsArgs] {
  val companion: ADAMCommandCompanion = VizReads

  def run(sc: SparkContext, job: Job): Unit = {
    VizReads.refName = args.refName

    val proj = Projection(contig, readName, start, cigar, primaryAlignment, firstOfPair, properPair, readMapped)
    VizReads.reads = sc.adamLoad(args.inputPath, projection = Some(proj))

    val server = new org.eclipse.jetty.server.Server(8080)
    val handlers = new org.eclipse.jetty.server.handler.ContextHandlerCollection()
    server.setHandler(handlers)
    handlers.addHandler(new org.eclipse.jetty.webapp.WebAppContext("adam-cli/src/main/webapp", "/"))
    server.start()
    println("View the visualization at: 8080")
    server.join()
  }

}
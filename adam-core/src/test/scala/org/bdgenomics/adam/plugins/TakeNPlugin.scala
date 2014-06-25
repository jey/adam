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
package org.bdgenomics.adam.plugins

import org.bdgenomics.adam.avro.ADAMRecord
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class TakeNPlugin extends ADAMPlugin[ADAMRecord, ADAMRecord] with Serializable {
  override def projection: Option[Schema] = None
  override def predicate: Option[(ADAMRecord) => Boolean] = None

  override def run(sc: SparkContext, recs: RDD[ADAMRecord], args: String): RDD[ADAMRecord] = {
    val n = args.toInt
    sc.parallelize(recs.take(n))
  }
}
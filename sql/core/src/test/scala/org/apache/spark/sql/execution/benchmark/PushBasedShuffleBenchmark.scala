/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure push-based shuffle performance.
 * To run this:
 * {{{
 *   1. without sbt:
 *        bin/spark-submit --jars <spark core test jar>,<spark catalyst test jar>
 *          --class <this class> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt
 *        "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/PushBasedShuffleBenchmark-results.txt".
 * }}}
 */
object PushBasedShuffleBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setMaster(System.getProperty("spark.sql.test.master", "local[1]"))
      .setAppName("test-sql-context")
      .set("spark.sql.shuffle.partitions", System.getProperty("spark.sql.shuffle.partitions", "4"))
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "3g")
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      // Enable push-based shuffle
      .set("spark.shuffle.push.enabled", "true")

    SparkSession.builder().config(conf).getOrCreate()
  }

  def runBenchmark(name: String, query: String): Unit = {
    val numRows = spark.range(1000000).count()
    val benchmark = new Benchmark("Push-based Shuffle Benchmark", numRows, 2, output = output)
    benchmark.addCase(name) { _ =>
      spark.sql(query).noop()
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    spark.range(1000000).createOrReplaceTempView("t1")

    val query = "SELECT a, count(*) FROM (SELECT id % 1000 AS a FROM t1) GROUP BY a"

    // Run with dynamic threads disabled
    spark.conf.set("spark.shuffle.push.dynamicThreads.enabled", "false")
    runBenchmark("Dynamic threads disabled", query)

    // Run with dynamic threads enabled
    spark.conf.set("spark.shuffle.push.dynamicThreads.enabled", "true")
    runBenchmark("Dynamic threads enabled", query)
  }
}

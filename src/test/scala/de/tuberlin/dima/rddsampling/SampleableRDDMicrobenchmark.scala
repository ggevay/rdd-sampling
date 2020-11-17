/*
 * Copyright 2020 Gábor E. Gévay
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.tuberlin.dima.rddsampling

import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.SparkContext
import org.apache.spark.benchmark._

object SampleableRDDMicrobenchmark extends BenchmarkBase {
  val seed = 0x1337
  val sc = new SparkContext(master = "local[4]", appName = "test")


  /**
   *
   * The result of the benchmarks can be viewed in the benchmarks/SampleableRDDenchmark-results.txt file.
   * WARNING: Benchmarks can take a long time to process. It might be useful to comment out unimportant benchmarks.
   */
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("Performance for decreasing sampling fraction") {
      benchmarkSampleFractions(numValues = 10000000, true)
    }

    runBenchmark("Performance for increasing number of samples") {
      benchmarkNumberSamples(numValues = 10000000, true)
    }

    runBenchmark("Performance for decreasing sampling fraction (No replacement)") {
      benchmarkSampleFractions(numValues = 10000000, false)
    }

    runBenchmark("Performance for increasing number of samples (No replacement)") {
      benchmarkNumberSamples(numValues = 10000000, false)
    }

  }


  private def benchmarkSampleFractions(numValues: Int, withReplacement: Boolean): Unit = {

    val benchmark = new Benchmark("Benchmark Setup", numValues, output = output)

    val fractions = Seq(1.0, 1.0 / 2, 1.0 / 4, 1.0 / 8, 1.0 / 16, 1.0 / 32, 1.0 / 64, 1.0 / 128)

    for (fraction <- fractions) {
      benchmarkSampleableRDD(benchmark, withReplacement, fraction, 1, numValues)
    }

    for (fraction <- fractions) {
      benchmarkRDD(benchmark, withReplacement, fraction, 1, numValues)
    }
    benchmark.run()
  }

  private def benchmarkNumberSamples(numValues: Int, withReplacement: Boolean): Unit = {

    val benchmark = new Benchmark("Benchmark Setup", numValues, output = output)

    val numSamples = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    for (numSample <- numSamples) {
      benchmarkSampleableRDD(benchmark, withReplacement, 0.001, numSample, numValues)
    }

    for (numSample <- numSamples) {
      benchmarkRDD(benchmark, withReplacement, 0.001, numSample, numValues)
    }
    benchmark.run();
  }

  def benchmarkRDD(benchmark: Benchmark, withReplacement: Boolean, fraction: Double, numSamples: Int, numValues: Int): Unit = {
    val rdd = sc.parallelize((1 to numValues)).cache().sample(withReplacement, fraction, seed)
    rdd.count() //this triggers the first execution which also stores data in the caches

    benchmark.addCase(s"RDD           withReplace: $withReplacement  frac: $fraction numIt: $numSamples",
      5) { _ =>
      for (_ <- 1 to numSamples) {
        rdd.count()
      }
    }
  }


  def benchmarkSampleableRDD(benchmark: Benchmark, withReplacement: Boolean, fraction: Double, numSamples: Int, numValues: Int): Unit = {
    val rdd = sc.parallelize((1 to numValues))
    val sample = SampleableRDD(rdd).cache().sample(withReplacement, fraction, seed)
    sample.count() //this triggers the first execution which also stores data in the caches

    benchmark.addCase(s"de.tuberlin.dima.rddsampling.SampleableRDD withReplace: $withReplacement  frac: $fraction numIt: $numSamples",
      5) { _ =>
      for (_ <- 1 to numSamples) {
        sample.count()
      }
    }

  }

}

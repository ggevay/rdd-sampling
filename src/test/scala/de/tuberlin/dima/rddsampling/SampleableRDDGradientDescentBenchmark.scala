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

import org.apache.spark.SparkContext
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{GradientDescent, LogisticGradient, SquaredL2Updater}
import org.apache.spark.rdd.RDD

import scala.util.Random

object SampleableRDDGradientDescentBenchmark extends BenchmarkBase {
  val sc = new SparkContext(master = "local[4]", appName = "test")

  /**
   * Main process of the whole benchmark.
   * Implementations of this method are supposed to use the wrapper method `runBenchmark`
   * for each benchmark scenario.
   */
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("Gradient descent sample size on RDD and de.tuberlin.dima.rddsampling.SampleableRDD performance") {
      benchmarkGradientDescentSize(numValues = 1000000)
    }

    runBenchmark("Gradient descent number of iterations on RDD and de.tuberlin.dima.rddsampling.SampleableRDD performance") {
      benchmarkGradientDescentIter(numValues = 1000000)
    }
  }

  private def benchmarkGradientDescentSize(numValues: Int) = {
    val benchmark = new Benchmark("Benchmark Setup", numValues, output = output)

    val batchSizes = Seq(0.1, 0.09, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, 0.02, 0.01)

    val numFeatures = 4

    val rdd = makeRDD(numValues, numFeatures).cache()
    rdd.count() //this triggers the first execution which also stores data in the caches

    val sampleabRDD = SampleableRDD(makeRDD(numValues, numFeatures)).cache()
    sampleabRDD.count() //this triggers the first execution which also stores data in the caches

    for (batchSize <- batchSizes) {
      benchmark.addCase(s"RDD           gradient descent Sample Size: $batchSize", 3) { _ => gradientDescent(numFeatures, batchSize, 1000, rdd) }
    }

    for (batchSize <- batchSizes) {
      benchmark.addCase(s"de.tuberlin.dima.rddsampling.SampleableRDD gradient descent Sample Size: $batchSize", 3) { _ => gradientDescent(numFeatures, batchSize, 1000, sampleabRDD) }
    }

    benchmark.run()
  }


  private def makeRDD(numValues: Int, numFeatures: Int) = {
    sc.parallelize(0 until numValues, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => (1.0, Vectors.dense(Array.fill(numFeatures)(random.nextDouble()))))
    }
  }

  private def benchmarkGradientDescentIter(numValues: Int) = {
    val benchmark = new Benchmark("Benchmark Setup", numValues, output = output)

    val numIters = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val numFeatures = 4

    val rdd = makeRDD(numValues, numFeatures).cache()
    rdd.count() //this triggers the first execution which also stores data in the caches

    val sampleabRDD = SampleableRDD(makeRDD(numValues, numFeatures)).cache()
    sampleabRDD.count() //this triggers the first execution which also stores data in the caches

    for (numIter <- numIters) {
      benchmark.addCase(s"RDD           gradient descent Iterations: $numIter", 3) { _ => gradientDescent(numFeatures, 0.0001, numIter, rdd) }
    }

    for (numIter <- numIters) {
      benchmark.addCase(s"de.tuberlin.dima.rddsampling.SampleableRDD gradient descent Iterations: $numIter", 3) { _ => gradientDescent(numFeatures, 0.0001, numIter, sampleabRDD) }
    }

    benchmark.run()
  }


  def gradientDescent(numFeatures: Int, miniBatchFraction: Double, numIter: Int, rdd: RDD[(Double, linalg.Vector)]) = {
    GradientDescent.runMiniBatchSGD(
      rdd,
      new LogisticGradient,
      new SquaredL2Updater,
      0.1,
      numIter,
      1.0,
      miniBatchFraction,
      Vectors.dense(new Array[Double](numFeatures)))
  }
}

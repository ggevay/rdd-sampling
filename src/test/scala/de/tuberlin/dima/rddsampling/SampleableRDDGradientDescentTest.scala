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

import SampleableRDDGradientDescentBenchmark.sc
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{GradientDescent, LogisticGradient, SquaredL2Updater}
import org.scalatest.FunSuite

import scala.util.Random

/**
 * Test if computational results from the de.tuberlin.dima.rddsampling.SampleableRDD.sampling() method is correct by checking the final weights of a gradient descent computation.
 */
class SampleableRDDGradientDescentTest extends FunSuite {

  test("GradientDescentLargeBatch") {
    val m = 10000
    val numFeatures = 4
    val rdd = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => (1.0, Vectors.dense(Array.fill(numFeatures)(random.nextDouble()))))
    }.cache()

    val sampleableRDD = SampleableRDD(rdd).cache()

    val (weightsRDD, lossRDD) = GradientDescent.runMiniBatchSGD(
      rdd,
      new LogisticGradient,
      new SquaredL2Updater,
      0.1,
      1000,
      1.0,
      0.5,
      Vectors.dense(new Array[Double](numFeatures)))

    val (weightsSRDD, lossSRDD) = GradientDescent.runMiniBatchSGD(
      sampleableRDD,
      new LogisticGradient,
      new SquaredL2Updater,
      0.1,
      1000,
      1.0,
      0.5,
      Vectors.dense(new Array[Double](numFeatures)))

    assert(weightsRDD.toArray.corresponds(weightsSRDD.toArray) { (a, b) => 1.0-(a/b) <= 0.01})
  }

  test("GradientDescentSmallBatch") {
    val m = 10000
    val numFeatures = 4
    val rdd = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => (1.0, Vectors.dense(Array.fill(numFeatures)(random.nextDouble()))))
    }.cache()

    val sampleableRDD = SampleableRDD(rdd).cache()

    val (weightsRDD, lossRDD) = GradientDescent.runMiniBatchSGD(
      rdd,
      new LogisticGradient,
      new SquaredL2Updater,
      0.1,
      1000,
      1.0,
      0.01,
      Vectors.dense(new Array[Double](numFeatures)))

    val (weightsSRDD, lossSRDD) = GradientDescent.runMiniBatchSGD(
      sampleableRDD,
      new LogisticGradient,
      new SquaredL2Updater,
      0.1,
      1000,
      1.0,
      0.01,
      Vectors.dense(new Array[Double](numFeatures)))

    assert(weightsRDD.toArray.corresponds(weightsSRDD.toArray) { (a, b) => 1.0-(a/b) <= 0.01})
  }


}

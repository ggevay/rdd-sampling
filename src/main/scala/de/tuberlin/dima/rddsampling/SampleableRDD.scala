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

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{NondeterministicRDD, OneToOneDependency, Partition, TaskContext}

import scala.reflect.ClassTag
import scala.util.Random

/**
 *
 * An RDD that stores data partitions in a single elements, allowing for more efficient sampling.
 *
 * @param partitionsRDD The underlying representation of the SampleableRDD as an RDD of partitions.
 * @tparam A the type of the original RDD elements
 */
class SampleableRDD[A: ClassTag](private val partitionsRDD: RDD[SampleableRDDPartition[A]])
  extends RDD[A](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  override def compute(part: Partition, context: TaskContext): Iterator[A] = {
    firstParent[SampleableRDDPartition[A]].iterator(part, context).next().iterator
  }

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * Return a sampled subset of this RDD.
   *
   * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
   * @param fraction        expected size of the sample as a fraction of this RDD's size
   *                        without replacement: probability that each element is chosen; fraction must be [0, 1]
   *                        with replacement: expected number of times each element is chosen; fraction must be greater
   *                        than or equal to 0
   * @param seed            seed for the random number generator
   */
  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): RDD[A] = {
    require(fraction >= 0,
      s"Fraction must be nonnegative, but got ${fraction}")

    if (withReplacement) {
      /**
       * fill the result with randomly pulled elements from the partitions.
       */
      partitionsRDD.flatMap({ part =>
        val rnd = new Random(seed)
        val n = (part.size * fraction).toInt
        Array.fill(n)(part.data(rnd.nextInt(part.size)))
      })
    } else {
      /**
       * Randomly pick and move points to the end of the dataset.
       * Final sample are the n rightmost elements.
       */
      partitionsRDD.flatMap({ part =>
        part.synchronized {
          val rnd = new Random(seed)
          val n = (part.size * fraction).toInt

          for (i <- 1 until n) {
            val rndIdx = rnd.nextInt(part.size - i)
            val buf = part.data(part.size - i)
            part.data(part.size - i) = part.data(rndIdx)
            part.data(rndIdx) = buf
          }

          part.data.takeRight(n)
        }
      })
    }
  }

  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

}

object SampleableRDD {

  /**
   * Constructs a SampleableRDD from an RDD by mapping each partition to a [[SampleableRDDPartition]].
   */
  def apply[A: ClassTag](elems: RDD[A]): SampleableRDD[A] = {
    // It's not deterministic because of rearranging the array
    val partitions = new NondeterministicRDD(elems.mapPartitions(iter => Iterator(SampleableRDDPartition(iter)),
      preservesPartitioning = false))
    new SampleableRDD[A](partitions)
  }

}

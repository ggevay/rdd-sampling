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
package org.apache.spark

import org.apache.spark.rdd.{DeterministicLevel, RDD}

import scala.reflect.ClassTag

class NondeterministicRDD[A: ClassTag](private val wrappedRDD: RDD[A])
  extends RDD[A](wrappedRDD.context, List(new OneToOneDependency(wrappedRDD))) {

  override def compute(split: Partition, context: TaskContext): Iterator[A] = wrappedRDD.compute(split, context)

  override protected def getPartitions: Array[Partition] = wrappedRDD.partitions

  override protected def getOutputDeterministicLevel: DeterministicLevel.Value = DeterministicLevel.INDETERMINATE
}

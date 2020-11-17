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

import scala.reflect.ClassTag

/**
 * A partition that keeps the data in an array enabling quick access.
 *
 * @param data An Array with all the data from its partition
 * @tparam A the type of the original RDD elements
 */
class SampleableRDDPartition[A](val data: Array[A]) extends Serializable {

  val size: Int = data.length

  def iterator: Iterator[A] = data.toIterator

}

object SampleableRDDPartition {

  /**
   * Builds a [[SampleableRDDPartition]] by retrieving and storing an array from the given iterator.
   */
  def apply[A: ClassTag](iterator: Iterator[A]): SampleableRDDPartition[A] = {
    new SampleableRDDPartition[A](iterator.toArray)
  }

}
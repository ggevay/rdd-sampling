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

/*
 * This code has been taken from Apache Spark and modified for custom
 * use by Krisk Sama (sama@campus.tu-berlin.de).
 */
package org.apache.spark.benchmark

import java.io.{File, FileOutputStream, OutputStream}

/**
 * A base class for generate benchmark results to a file.
 */
abstract class BenchmarkBase {
  var output: Option[OutputStream] = None

  /**
   * Main process of the whole benchmark.
   * Implementations of this method are supposed to use the wrapper method `runBenchmark`
   * for each benchmark scenario.
   */
  def runBenchmarkSuite(mainArgs: Array[String]): Unit

  final def runBenchmark(benchmarkName: String)(func: => Any): Unit = {
    val separator = "=" * 96
    val testHeader = (separator + '\n' + benchmarkName + '\n' + separator + '\n' + '\n').getBytes
    output.foreach(_.write(testHeader))
    func
    output.foreach(_.write('\n'))
  }

  def main(args: Array[String]): Unit = {
    val resultFileName = s"${this.getClass.getSimpleName.replace("$", "")}-results.txt"
    val file = new File(s"benchmarks/local/$resultFileName")
    if (!file.exists()) {
      file.getParentFile.mkdirs()
      file.createNewFile()
    }
    output = Some(new FileOutputStream(file))

    runBenchmarkSuite(args)

    output.foreach { o =>
      if (o != null) {
        o.close()
      }
    }

    afterAll()
  }

  /**
   * Any shutdown code to ensure a clean shutdown
   */
  def afterAll(): Unit = {}
}

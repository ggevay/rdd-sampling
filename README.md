# Efficient Sampling from RDDs

An efficient implementation of repeated RDD sampling.

For small sampling ratios, Spark's built-in `RDD.sample` method is inefficient because it scans the whole RDD. This repo contains code to avoid doing such a scan repeatedly when the sampling is performed at every iteration of a loop (e.g., in a mini-batch gradient descent). The trick is to create a special RDD (`SampleableRDD`) before the loop, having one big element per partition. Each of these big elements stores the elements of one partition of the original RDD as an array. `SampleableRDD` can be efficiently sampled because we can do random access on the arrays.

## Usage

```scala
// We have an RDD of any type:
val rdd = sc.parallelize((1 to 1000))

// Create SampleableRDD before a loop and cache it:
val sampleableRDD = SampleableRDD(rdd).cache()

while (...) {
  // Efficiently sample (run time proportional to the sample size):
  val samp = sampleableRDD.sample(withReplacement = true, fraction = 0.01, seed = 0x1337)
  // ...
}
```

## Caveats

* It is important that the `SampleableRDD` is cached in a NON-serialized form. (Simply calling `cache` does this correctly by default.)
* If the tasks of `sampleableRDD.sample` are scheduled non-locally then we lose the performance benefits. You can check on the web UI that most of the tasks have Locality level "PROCESS_LOCAL".

## Authors

Implementation by [@samakk](https://github.com/samakk) at a TU Berlin course, idea by [@ggevay](https://github.com/ggevay). Inspired by several other RDDs that do the same trick of having one big object per partition ([IndexedRDD](https://github.com/amplab/spark-indexedrdd), [MapWithStateRDD](https://github.com/apache/spark/blob/v3.0.1/streaming/src/main/scala/org/apache/spark/streaming/rdd/MapWithStateRDD.scala), [BigDatalog's SetRDD](https://github.com/ashkapsky/BigDatalog/blob/f7809840cc3c5ad3ed67db480f2ed7ed2b9ac2e2/datalog/src/main/scala/edu/ucla/cs/wis/bigdatalog/spark/execution/setrdd/SetRDD.scala)).
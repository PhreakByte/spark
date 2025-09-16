# Apache Spark

Spark is a unified analytics engine for large-scale data processing. It provides
high-level APIs in Scala, Java, Python, and R (Deprecated), and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
pandas API on Spark for pandas workloads, MLlib for machine learning, GraphX for graph processing,
and Structured Streaming for stream processing.

- Official version: <https://spark.apache.org/>
- Development version: <https://apache.github.io/spark/>

[![GitHub Actions Build](https://github.com/apache/spark/actions/workflows/build_main.yml/badge.svg)](https://github.com/apache/spark/actions/workflows/build_main.yml)
[![PySpark Coverage](https://codecov.io/gh/apache/spark/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/spark)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/pyspark?period=month&units=international_system&left_color=black&right_color=orange&left_text=PyPI%20downloads)](https://pypi.org/project/pyspark/)


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](https://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.

## Building Spark

Spark is built using [Apache Maven](https://maven.apache.org/).
To build Spark and its example programs, run:

```bash
./build/mvn -DskipTests clean package
```

(You do not need to do this if you downloaded a pre-built package.)

More detailed documentation is available from the project site, at
["Building Spark"](https://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see ["Useful Developer Tools"](https://spark.apache.org/developer-tools.html).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

```bash
./bin/spark-shell
```

Try the following command, which should return 1,000,000,000:

```scala
scala> spark.range(1000 * 1000 * 1000).count()
```

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

```bash
./bin/pyspark
```

And run the following command, which should also return 1,000,000,000:

```python
>>> spark.range(1000 * 1000 * 1000).count()
```

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

```bash
./bin/run-example SparkPi
```

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be spark:// URL,
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" to run locally with N threads. You
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

```bash
MASTER=spark://host:7077 ./bin/run-example SparkPi
```

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

```bash
./dev/run-tests
```

Please see the guidance on how to
[run tests for a module, or individual tests](https://spark.apache.org/developer-tools.html#individual-tests).

There is also a Kubernetes integration test, see resource-managers/kubernetes/integration-tests/README.md

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version and Enabling YARN"](https://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.

## Repository Map

This document provides a detailed overview of the Apache Spark repository, including its architecture, key components, and functionalities.

### 1. Overall Architecture

Apache Spark is a unified analytics engine for large-scale data processing. It provides high-level APIs in Scala, Java, Python, and R, and an optimized engine that supports general computation graphs for data analysis.

The Spark architecture consists of the following components:

*   **Driver Program:** The process running the main function of the application and creating the `SparkContext`. The driver is responsible for converting the user's code into a physical execution plan and scheduling tasks on the executors.
*   **Cluster Manager:** An external service for acquiring resources on the cluster (e.g., Standalone, YARN, Mesos, Kubernetes).
*   **Executors:** Worker processes on the cluster that execute tasks. Each executor has its own memory and can run multiple tasks in parallel.

The driver program communicates with the cluster manager to request resources. The cluster manager then launches executors on the worker nodes. The driver sends tasks to the executors for execution. The executors run the tasks and send the results back to the driver.

### 2. Core Module (`spark-core`)

The core module provides the fundamental functionality of Spark, including the `SparkContext`, RDDs, and the scheduler.

#### 2.1. `SparkContext`

The `SparkContext` is the main entry point for Spark functionality. It represents a connection to a Spark cluster and is used to create RDDs, accumulators, and broadcast variables.

*   **File:** `core/src/main/scala/org/apache/spark/SparkContext.scala`
*   **Key Responsibilities:**
    *   Initializing the Spark environment.
    *   Connecting to the cluster manager.
    *   Creating RDDs.
    *   Managing shared variables.
    *   Submitting jobs to the scheduler.

#### 2.2. RDDs (Resilient Distributed Datasets)

RDDs are the fundamental data structure of Spark. An RDD is an immutable, partitioned collection of elements that can be operated on in parallel.

*   **File:** `core/src/main/scala/org/apache/spark/rdd/RDD.scala`
*   **Key Characteristics:**
    *   **Immutable:** Once an RDD is created, it cannot be changed.
    *   **Distributed:** The data in an RDD is partitioned and distributed across the nodes in the cluster.
    *   **Resilient:** RDDs are fault-tolerant. If a partition is lost, it can be recomputed from its lineage.
*   **Operations:** RDDs support two types of operations:
    *   **Transformations:** Create a new RDD from an existing one (e.g., `map`, `filter`).
    *   **Actions:** Return a value to the driver program after running a computation on the RDD (e.g., `reduce`, `collect`).

#### 2.3. Scheduler (`DAGScheduler` and `TaskScheduler`)

The scheduler is responsible for executing tasks on the cluster. It consists of two main components: the `DAGScheduler` and the `TaskScheduler`.

*   **`DAGScheduler`:**
    *   **File:** `core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala`
    *   **Responsibilities:**
        *   Takes a logical plan (a DAG of RDDs) and divides it into a set of stages.
        *   Submits stages as `TaskSets` to the `TaskScheduler`.
        *   Handles failures due to lost shuffle outputs.
*   **`TaskScheduler`:**
    *   **File:** `core/src/main/scala/org/apache/spark/scheduler/TaskScheduler.scala`
    *   **File (Implementation):** `core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala`
    *   **Responsibilities:**
        *   Launches tasks on the cluster.
        *   Retries failed tasks.
        *   Reports task statuses back to the `DAGScheduler`.
        *   Handles data locality to optimize task placement.

### 3. SQL Module (`spark-sql`)

The SQL module provides support for structured data processing with DataFrames and SQL.

#### 3.1. `SparkSession`

The `SparkSession` is the entry point for Spark SQL. It provides a unified interface for working with structured data.

*   **File:** `sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala`
*   **Key Responsibilities:**
    *   Creating `DataFrame`s and `Dataset`s.
    *   Executing SQL queries.
    *   Managing the Spark SQL catalog.

#### 3.2. `Dataset` and `DataFrame`

`Dataset`s and `DataFrame`s are the primary data abstractions for structured data in Spark.

*   **File (`Dataset`):** `sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala`
*   **`DataFrame`:** A `DataFrame` is a `Dataset[Row]`, which is an untyped view of a `Dataset`.
*   **Key Characteristics:**
    *   **Structured:** Data is organized into named columns.
    *   **Optimized:** `Dataset` and `DataFrame` operations are optimized by the Catalyst optimizer.
    *   **Typed (Dataset):** `Dataset`s are strongly-typed, which provides compile-time type safety.

#### 3.3. Catalyst Optimizer

The Catalyst optimizer is the query optimizer that powers Spark SQL. It optimizes the logical plan of a `DataFrame` or `Dataset` and generates an efficient physical plan for execution.

### 4. Python API (`pyspark`)

PySpark is the Python API for Spark. It allows you to write Spark applications in Python.

#### 4.1. Py4J Interaction

PySpark uses Py4J to communicate with the JVM. The Python `SparkContext` and other PySpark objects are wrappers around their corresponding Java objects.

*   **File:** `python/pyspark/java_gateway.py`

#### 4.2. `SparkContext`, `SparkSession`, and `DataFrame` in Python

PySpark provides Python implementations of the core Spark APIs.

*   **`SparkContext`:** `python/pyspark/context.py`
*   **`SparkSession`:** `python/pyspark/sql/session.py`
*   **`DataFrame`:** `python/pyspark/sql/dataframe.py`

### 5. Machine Learning Library (`spark-mllib`)

MLlib is Spark's machine learning library. It provides a wide range of machine learning algorithms and utilities.

#### 5.1. Algorithm Types

MLlib provides algorithms for various machine learning tasks, including:

*   Classification
*   Regression
*   Clustering
*   Collaborative filtering
*   Frequent pattern mining

#### 5.2. ML Pipelines API

The ML Pipelines API provides a set of high-level APIs for building, evaluating, and tuning machine learning pipelines.

*   **`Pipeline`:** A pipeline consists of a sequence of stages.
*   **`Transformer`:** An algorithm that can transform one `DataFrame` into another.
*   **`Estimator`:** An algorithm which can be fit on a `DataFrame` to produce a `Transformer`.

### 6. Sample Scripts

Here are some sample scripts to demonstrate the usage of the different Spark components.

#### Core API (RDDs):

```python
from pyspark import SparkContext

sc = SparkContext("local", "RDD Example")
rdd = sc.parallelize([1, 2, 3, 4, 5])
squared_rdd = rdd.map(lambda x: x * x)
print(squared_rdd.collect())
# Output: [1, 4, 9, 16, 25]
sc.stop()
```

#### SQL API (DataFrames):

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()
df = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob")
], ["id", "name"])
df.show()
# +---+-----+
# | id| name|
# +---+-----+
# |  1|Alice|
# |  2|  Bob|
# +---+-----+
spark.stop()
```

#### MLlib API (Pipelines):

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PipelineExample").getOrCreate()

# Prepare training data from a list of (id, text, label) tuples.
training = spark.createDataFrame([
    (0, "a b c d e spark", 1.0),
    (1, "b d", 0.0),
    (2, "spark f g h", 1.0),
    (3, "hadoop mapreduce", 0.0)
], ["id", "text", "label"])

# Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.001)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# Fit the pipeline to training documents.
model = pipeline.fit(training)

# Prepare test documents, which are unlabeled (id, text) tuples.
test = spark.createDataFrame([
    (4, "spark i j k"),
    (5, "l m n"),
    (6, "spark hadoop spark"),
    (7, "apache hadoop")
], ["id", "text"])

# Make predictions on test documents and print columns of interest.
prediction = model.transform(test)
selected = prediction.select("id", "text", "probability", "prediction")
for row in selected.collect():
    rid, text, prob, prediction = row
    print(f"({rid}, {text}) --> prob={prob}, prediction={prediction}")

spark.stop()
```

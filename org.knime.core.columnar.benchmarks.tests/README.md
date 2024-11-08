# Benchmarks

This folder contains JMH benchmarks for the KNIME Core Columnar project, aimed at evaluating the performance and efficiency of various columnar data processing operations within the KNIME Analytics Platform.

## Overview

The benchmarks are designed to provide insights into the performance of columnar data operations, helping to optimize processing within the KNIME Analytics Platform.

## Structure

- `org.knime.core.columnar.BenchmarkRunner`: The main entry point for running the benchmarks.
- `org.knime.core.columnar.*`: Contains micro-benchmarks for individual components of the columnar backend.
- `org.knime.core.columnar.workflow`: Contains benchmarks based on KNIME workflows.
- `workflows/`: Directory containing the benchmark workflows and associated data files.

## Running Benchmarks Locally

To execute the benchmarks, run the following command in the repository’s root directory:

```sh
mvn clean verify -Pbenchmark
```

### Viewing Results

- After running the benchmarks locally, find the results at `org.knime.core.columnar.benchmarks.tests/target/surefire-reports/benchmark-results.json`.
- To visualize the results, upload the JSON file to [https://jmh.morethan.io/](https://jmh.morethan.io/).
- If using Jenkins, the Jenkins job will include a **JMH Report** tab for viewing the results directly.

## Tips for Writing Benchmarks

- Modify `BenchmarkRunner.INCLUDE_BENCHMARKS` to select specific benchmarks, allowing for faster feedback when testing.
- Ensure that all benchmarks within a class use consistent parameters. The JMH Visualizer requires this for accurate results ([reference](https://github.com/jzillmann/jmh-visualizer/issues/38#issuecomment-1072569073)).

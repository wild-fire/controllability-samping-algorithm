# Controllability Sampling Algorithm

This projects aims to implement in Hadoop the algorith described on Jia and Barabasi's paper:

> Jia, T., & Barabási, A. L. (2013).
> Control capacity and a random sampling method in exploring controllability of complex networks.
> Scientific reports, 3.

In this paper the authors study how to measure how driver (important) is a node in a network and the propose an algorithm that provides, for each node of a network, in how many configurations of driver nodes contain that particular node.

A configuration of driver nodes is one that allows you to control the whole network, so knowing that a node is present in 80% of all the configurations allows you to assume that node is more important that other nodes that is only present in 10% of the configurations.

Since obtaining all the configurations can be prohibitive for large graphs due to its time complexity, the authros propose an algorithm that starts with a random driver node configuration and then begin to perform some random permutations following a markov chain which, because of its mathematical properties, allow the authors to obtain a non biased sampling of configurations.

## Requirements

This project depends on the Hadoop 2.6.0 client library and it has been included in the Maven descriptor file (pom.xml) so, if you have Maven in your system, all you need to do is

```
mvn install
```

If you have no Maven you can downolad it from http://maven.apache.org/ and maybe take a look at this quick maven tutorial: http://maven.apache.org/guides/getting-started/maven-in-five-minutes.html

## Basic Usage

On the jar folder you can find compiled jar files with all the classes required to run the jobs. To run them you just have to use the following command:

```
hadoop jar target/controllability-samping-algorithm-VERSION.jar jobs.SamplingAlgorithm path-to-graph.tsv tmp-folder output-folder
```

### Input format

The input must be a TSV file in which the first two columns have the target node and the source node. BE CAREFUL WITH THIS! A file containing:

```
1\t2
1\t3
2\t3
4\t2
```

Represent a graph where `2` is connected to `1` and `4`, and `3` is connected to `1` and `2`.

### Output format

The output is a TSV file where the first column is the node and the second one the number of times this node is present on a configuration

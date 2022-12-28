# SparkEC: speeding up alignment-based DNA error correction

SparkEC is a parallel tool that allows to correct DNA sequencing errors on NGS datasets. It is implemented upon the [Apache Spark](https://spark.apache.org) Big Data framework.

This project is based on the [CloudEC](https://github.com/CSCLabTW/CloudEC) tool. The underlying Multiple Sequence Alignment (MSA) algorithms provided by CloudEC keep being the same in SparkEC, so their correction accuracy is ensured, but the code architecture has been completely refactored and the Apache Hadoop framework replaced by Spark. Other optimizations provided by SparkEC include the split-based system using a two-step k-mers distribution, the avoidance of preprocessing of the input datasets and the use of more memory-efficient data structures.

### Citation

If you use **SparkEC** in your research, please cite our work using the following reference:

> Roberto R. Expósito, Marco Martínez-Sánchez, Juan Touriño. [SparkEC: speeding up alignment-based DNA error correction tools](https://doi.org/10.1186/s12859-022-05013-1). BMC Bioinformatics 23(1): 464:1-464:17 (2022).

## Quick Start Guide

### Prerequisites

This project requires the following software to run:
* Apache Spark framework version 2.0 (or above). All you need is to have the *spark-submit* command available on your system PATH.
* Java Runtime Environment (JRE) version 1.8 (or above) compatible with Spark. All you need is to have the *java* command available on your system PATH, or the JAVA_HOME environment variable pointing to your JRE installation.

To take advantage of the [Hadoop Distributed File System (HDFS)](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) to store and process the input datasets on a distribued manner, you also need Apache Hadoop version 2.8 (or above).

Download SparkEC and unzip the tarball or, alternatively, clone the GitHub repository by executing:

`git clone https://github.com/UDC-GAC/SparkEC.git`

### Execution

SparkEC must be executed by using the *spark-submit* command provided by Spark, which launches the Spark jobs to the cluster. The general syntax is as follows:

`spark-submit target/SparkEC.jar -in <input> -out <output>`


### Configuration

SparkEC configuration can be tuned through a Java properties file. As a template, SparkEC provides a default properties file (*config.properties.template*) at the *conf* directory. Once the configuration is set, it can be used with the "-config" command-line argument as follows:

`spark-submit target/SparkEC.jar -in <input> -out <output> -config <config_file>`

It may be also interesting to tune the Spark configuration in order to get the best performance. For instance:

* **spark.hadoop.validateOutputSpecs:** this option must be set to *false* if the output of individual phases is enabled.
* **spark.serializer:** it is highly recommended to set this option to *org.apache.spark.serializer.KryoSerializer* in order to take advantage of the Kryo serializer.


## Compilation

In case you need to recompile the SparkEC source code, the prerequisites are:
* Java Develpment Kit (JDK) version 1.8 (or above).
* Apache Maven version 3.1 (or above).
* [Hadoop Sequence Parser (HSP)](https://github.com/UDC-GAC/hsp) library.

In order to build the executable JAR file, simply execute the following Maven command from within the SparkEC root directory:

`mvn package`

After a successful compilation, the resulting JAR file (*SparkEC.jar*) will be generated at the *target* directory.


## Authors

SparkEC is developed in the [Computer Architecture Group](https://gac.udc.es/?page_id=770&lang=en) at the [Universidade da Coruña](https://www.udc.es/en) by:

* **Roberto R. Expósito** (https://gac.udc.es/~rober)
* **Marco Martínez-Sánchez** (https://orcid.org/0000-0003-2444-9112)
* **Juan Touriño** (http://gac.udc.es/~juan)


## License

This tool is distributed as free software and is publicly available under the GPLv3 license (see the [LICENSE](LICENSE) file for more details).

# SparkEC: speeding up alignment-based DNA error correction

SparkEC is an error correction parallel tool whose goal is to correct DNA Sequencing errors. It is implemented upon the [Apache Spark](https://spark.apache.org) Big Data framework.

This project is based on the [CloudEC](https://github.com/CSCLabTW/CloudEC) tool. The underlying Multiple Sequence Alignment (MSA) algorithms provided by CloudEC keep being the same in SparkEC, so their correction accuracy is ensured, but the code architecture has been completely refactored and the Apache Hadoop framework replaced by Spark. Other optimizations provided by SparkEC include the split-based system, the avoidance of preprocessing of the input datasets and the use of more memory-efficient data structures.


## Getting Started

### Prerequisites

This project requires the following software to run:
* Apache Spark framework version 2.0 (or above).
* Java Runtime Environment (JRE) version 1.8 (or above) compatible with Spark.

Download SparkEC and unzip the tarball or, alternatively, clone the GitHub repository by executing:

`git clone https://github.com/UDC-GAC/SparkEC.git`

### Execution

This tool can be executed by submitting it as a Spark job using the *spark-submit* command:

`spark-submit target/SparkEC.jar -in <input dataset> -out <output directory>`


## Configuration

SparkEC settings can be set through the *config.properties.template* file provided at the *conf* directory. Check this file to see all the available configuration parameters for this tool. Once the configuration is set, it can be used with the "-config" command-line argument as follows:

`spark-submit target/SparkEC.jar -in <input dataset> -out <output directory> -config <configuration file>`

It may be also interesting to tune the Spark configuration in order to get the best performance. For instance:

* **spark.hadoop.validateOutputSpecs:** this option must be set to *false* if the output of individual phases is enabled.
* **spark.serializer:** it is highly recommended to set this option to *org.apache.spark.serializer.KryoSerializer* in order to take advantage of the Kryo serializer.


## Compilation

The prerequisites to build SparkEC are:
* Java Develpment Kit (JDK) version 1.8 (or above).
* Apache Maven version 3 (or above).
* [Hadoop Sequence Parser](https://github.com/UDC-GAC/hsp) library.

In order to build the project, simply run the required Maven phase. For example:

`mvn package`

The resulting *jar* file to run SparkEC will be generated at the *target* directory, with the name *SparkEC.jar*. Note that the first time you execute the previous command, Maven will download all the plugins and related dependencies it needs. From a clean installation of Maven, this can take quite a while.


## Authors

SparkEC is developed in the [Computer Architecture Group](https://gac.udc.es/?page_id=770&lang=en) at the [Universidade da Coruña](https://www.udc.es/en) by:

* **Roberto R. Expósito** (https://gac.udc.es/~rober)
* **Marco Martínez-Sánchez** (https://orcid.org/0000-0003-2444-9112)
* **Juan Touriño** (http://gac.udc.es/~juan)


## License

This tool is distributed as free software and is publicly available under the GPLv3 license (see the [LICENSE](LICENSE) file for more details).

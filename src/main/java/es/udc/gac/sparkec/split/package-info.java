/**
 * <p>
 * This package contains all the needed classes to handle the Split optimization.
 * 
 * <p>
 * This performance optimization will prevent the Phases to simultaneously emit all the kmers, and just
 * emit them split by split, keeping the total memory usage under a threshold. This way, Spark is able
 * to not use the disk during the execution of the Phase, speeding up the runtime.
 */
package es.udc.gac.sparkec.split;
package es.udc.gac.sparkec;

import java.io.Serializable;
import java.util.List;
import java.util.Stack;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.SizeEstimator;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.split.IPhaseSplitStrategy;
import es.udc.gac.sparkec.split.PhaseSplitStrategy;
import scala.Tuple2;

/**
 * Internal class to store a single RDD reference.
 *
 * @param <R> The key data type for the RDD
 * @param <S> The value data type for the RDD
 */
class PhaseData<R, S> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The RDD reference.
	 */
	private final JavaPairRDD<R, S> data;
	
	/**
	 * Whether this RDD should be output to disk as an intermediate result.
	 */
	private final boolean outputToDisk;
	
	/**
	 * The name of the phase that generated this RDD.
	 */
	private final String phaseName;
	
	/**
	 * The path where this RDD should be output, if needed.
	 */
	private final String outputPath;

	/**
	 * Constructor for the PhaseData, when this RDD is being used as an intermediate result.
	 * @param phaseName The name of the phase that generated this RDD
	 * @param data The RDD reference
	 * @param outputPath The output path for this RDD
	 */
	public PhaseData(String phaseName, JavaPairRDD<R, S> data, String outputPath) {
		this.data = data;
		this.outputToDisk = true;
		this.phaseName = phaseName;
		this.outputPath = outputPath;
	}

	/**
	 * Constructor for the PhaseData, when this RDD is not being used as an intermediate result.
	 * @param phaseName The name of the phase that generated this RDD
	 * @param data The RDD reference
	 */
	public PhaseData(String phaseName, JavaPairRDD<R, S> data) {
		this.data = data;
		this.outputToDisk = false;
		this.phaseName = phaseName;
		this.outputPath = "";
	}

	/**
	 * Returns the reference of the RDD contained by this PhaseData.
	 * @return The RDD reference
	 */
	public JavaPairRDD<R, S> getData() {
		return data;
	}

	/**
	 * Checks whether this RDD should be output to disk as an intermediate result.
	 * @return Whether this RDD should be output to disk as an intermediate result.
	 */
	public boolean getOutputToDisk() {
		return outputToDisk;
	}

	/**
	 * Gets the name of the phase that generated this RDD.
	 * @return The name of the phase that generated this RDD.
	 */
	public String getPhaseName() {
		return phaseName;
	}

	/**
	 * Gets the path to use when this RDD is being output as an intermediate result. 
	 * @return The path to use when this RDD is being output as an intermediate result.
	 */
	public String getOutputPath() {
		return outputPath;
	}

}

/**
 * <p>
 * Container for all the datasets that are going to be generated through all the phases being run through
 * the execution of SparkEC.
 * 
 * <p>
 * This class will keep track to all the temporary RDDs that need to be output to disk as intermediate results,
 * the ID mappings needed to regenerate the original format of the RDDs, and the information needed to estimate
 * the number of splits needed to get SparkEC to not use more disk than the needed. 
 */
public class Data implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LogManager.getLogger();

	private static final int SIZE_ESTIMATION_SAMPLE_COUNT = 10;
	private static final int MAXIMUM_PARTITION_SIZE = 1024 * 1024; // 1 MB

	/**
	 * Stack containing the RDD references for each phase.
	 */
	private Stack<PhaseData<Long, Node>> data;
	
	/**
	 * The first RDD read by the system.
	 */
	private PhaseData<Long, Node> startingRDD;

	/**
	 * The mappings of the internal numeric IDs being used, with the String IDs
	 * of the input.
	 */
	private JavaPairRDD<Long, String> idMapping;

	/**
	 * The resulting output RDD after all computations.
	 */
	private JavaRDD<String> outputRDD;

	/**
	 * The base path used to output all the temporary RDDs that were generated and need to be
	 * output to HDFS.
	 */
	private final String tmpOutput;
	
	/**
	 * The output path for the final SparkEC result.
	 */
	private final String output;

	/**
	 * Whether the final output for SparkEC has already been set.
	 */
	private boolean finalOutputSet;

	/**
	 * The current split strategy being used to handle the stored datasets. This strategy contains
	 * stats about the first dataset read by SparkEC.
	 */
	private IPhaseSplitStrategy splitStrategy;

	/**
	 * Returns the latest stored data into this container.
	 * @return The latest stored data into this container
	 */
	public JavaPairRDD<Long, Node> getLatestData() {
		return data.peek().getData();
	}

	/**
	 * Returns the ID mapping RDD.
	 * @return The ID mapping RDD
	 */
	public JavaPairRDD<Long, String> getMapping() {
		return idMapping;
	}

	/**
	 * Returns the split strategy currently being used.
	 * @return The split strategy currently being used.
	 */
	public IPhaseSplitStrategy getSplitStrategy() {
		return this.splitStrategy;
	}

	/**
	 * Sets the ID RDD mapping.
	 * @param mapping The ID RDD mapping
	 */
	public void setMapping(JavaPairRDD<Long, String> mapping) {
		this.idMapping = mapping;
	}

	/**
	 * Returns the number of temporary results currently stored in this container.
	 * @return The number of temporary result stored in this container.
	 */
	public int getNumElems() {
		return data.size();
	}

	/**
	 * Outputs to HDFS all the data stored by this container.
	 */
	public void outputData() {
		for (PhaseData<Long, Node> e : data) {
			if (e.getOutputToDisk()) {
				e.getData().saveAsTextFile(tmpOutput + "/" + e.getOutputPath());
			}
		}

		if (finalOutputSet) {
			outputRDD.saveAsTextFile(output);
		}
	}

	/**
	 * Adds a temporary result, that is not going to be output to disk.
	 * @param phaseName The name that generated this dataset
	 * @param data The dataset
	 */
	public void addTmpData(String phaseName, JavaPairRDD<Long, Node> data) {
		this.insertData(phaseName, data, "");
	}

	/**
	 * Adds a temporary result, that is going to be output to disk.
	 * @param phaseName The name that generated this dataset
	 * @param data The dataset
	 * @param outputPath The temporary path used to output this dataset, relative to the base temporary
	 * path.
	 */
	public void addOutputData(String phaseName, JavaPairRDD<Long, Node> data, String outputPath) {

		this.insertData(phaseName, data, outputPath);
	}

	/**
	 * Estimates the number of bytes used by a node RDD
	 * 
	 * @param rdd The node RDD
	 * @return The estimated size
	 */
	private double estimateSize(JavaPairRDD<Long, Node> rdd) {

		List<Tuple2<Long, Node>> sample = rdd.takeSample(true, SIZE_ESTIMATION_SAMPLE_COUNT);

		double estimated = 0L;
		for (int i = 0; i < sample.size(); i++) {
			estimated += SizeEstimator.estimate(sample.get(i));
		}
		estimated /= sample.size();

		estimated *= rdd.count();

		return estimated;
	}

	/**
	 * Initialized the split strategy for this container, using the given RDD.
	 * @param data The RDD to use to estimate the split strategy being used
	 */
	private void initializeSplitStrategy(JavaPairRDD<Long, Node> data) {
		this.splitStrategy.initialize(data.takeSample(true, 1).get(0)._2.getSeq().length(), data.count());
	}

	/**
	 * Internal method to insert data into the container.
	 * @param phaseName The name of the phase that generated the dataset
	 * @param data The dataset
	 * @param outputPath The temporary output path of the dataset, if any
	 */
	private void insertData(String phaseName, JavaPairRDD<Long, Node> data, String outputPath) {

		if (this.data.isEmpty()) {
			data = data.partitionBy(new HashPartitioner(data.getNumPartitions()));

			double size = estimateSize(data);

			double partitionSize = size / data.getNumPartitions();

			if (partitionSize > MAXIMUM_PARTITION_SIZE) {
				logger.warn("The partition size is too high for this dataset! It is recommended to keep it under 1MB.");
				logger.warn("The detected partition size was: " + partitionSize + ".");
			}

			this.initializeSplitStrategy(data);

		}

		data = data.persist(StorageLevel.MEMORY_ONLY()).setName(phaseName);
		data.count();
		PhaseData<Long, Node> d = new PhaseData<>(phaseName, data, outputPath);

		if (this.data.isEmpty()) {
			this.startingRDD = d;
		}

		if ((!this.data.isEmpty()) && (this.data.peek().getOutputPath().equals(""))) {
			// If there is no plan to output this phase result, remove its reference

			PhaseData<Long, Node> pd = this.data.pop();
			pd.getData().unpersist(false);

		}
		this.data.push(d);
	}
	
	/**
	 * Returns the first RDD read by the system.
	 * @return The first RDD read by the system
	 */
	public JavaPairRDD<Long, Node> getStartingRDD() {
		return this.startingRDD.getData();
	}

	/**
	 * Returns the final output RDD of the execution.
	 * @return The final output RDD of the execution
	 */
	public JavaRDD<String> getOutputRDD() {
		return outputRDD;
	}

	/**
	 * Sets the final output RDD of the execution.
	 * @param outputRDD The final output RDD of the execution
	 */
	public void setOutputRDD(JavaRDD<String> outputRDD) {
		this.outputRDD = outputRDD;
		this.finalOutputSet = true;
	}

	/**
	 * Default constructor for the Data RDD container.
	 * @param outputPath The output path of the resulting RDD
	 * @param tmpPath The base path for the phase temporary results
	 * @param memoryAvailable The memory available for RDD storage purposes, used for split estimation
	 * @param memoryConstant The constant used for the memory estimation step
	 * @param k The k used by the algorithm, used for split memory estimation
	 */
	public Data(String outputPath, String tmpPath, long memoryAvailable, float memoryConstant, int k) {
		this.tmpOutput = tmpPath;
		this.output = outputPath;
		this.finalOutputSet = false;
		this.data = new Stack<>();
		this.startingRDD = null;
		this.outputRDD = null;
		this.splitStrategy = new PhaseSplitStrategy(k, memoryAvailable, memoryConstant);
	}

}

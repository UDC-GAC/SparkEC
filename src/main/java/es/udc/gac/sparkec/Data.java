package es.udc.gac.sparkec;

import java.io.Serializable;
import java.util.List;
import java.util.Stack;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

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
		this.outputPath = null;
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

	/**
	 * Stack containing the RDD references for each phase.
	 */
	private Stack<PhaseData<Long, Node>> data;

	/**
	 * The first RDD read by the system.
	 */
	private PhaseData<Long, Node> inputRDD;

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
	 * The configuration being currently used in this execution.
	 */
	private Config config;

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
		idMapping = mapping;
		idMapping.persist(StorageLevel.MEMORY_ONLY_SER());
		idMapping.count();
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

		if (finalOutputSet && outputRDD != null) {
			outputRDD.saveAsTextFile(output);
		}
	}

	/**
	 * Adds a temporary result, that is not going to be output to disk.
	 * @param phaseName The name that generated this dataset
	 * @param data The dataset
	 */
	public void addTmpData(String phaseName, JavaPairRDD<Long, Node> data) {
		this.insertData(phaseName, data, null);
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
	 * Initialized the split strategy for this container, using the given RDD.
	 * @param data The RDD to use to estimate the split strategy being used
	 */
	private void initializeSplitStrategy(JavaPairRDD<Long, Node> data) {
		int seqLen = config.getSeqLen();
		long count = config.getNumSeq();

		if (config.getSeqLen() <= 0) {
			List<Tuple2<Long, Node>> sample = data.takeSample(true, SIZE_ESTIMATION_SAMPLE_COUNT);
			seqLen = 0;
			for (int i = 0; i < sample.size(); i++) {
				seqLen += sample.get(i)._2.getSeq().length();
			}
			if (seqLen > 0)
				seqLen /= sample.size();
		}

		if (config.getNumSeq() <= 0)
			count = data.count();

		logger.info("seqLen = "+seqLen);
		logger.info("numSeq = "+count);
		this.splitStrategy.initialize(seqLen, count);
	}

	/**
	 * Internal method to insert data into the container.
	 * @param phaseName The name of the phase that generated the dataset
	 * @param data The dataset
	 * @param outputPath The temporary output path of the dataset, if any
	 */
	private void insertData(String phaseName, JavaPairRDD<Long, Node> data, String outputPath) {

		if (this.data.isEmpty())
			this.initializeSplitStrategy(data);

		if (data != null) {
			data.persist(StorageLevel.MEMORY_ONLY_SER()).setName(phaseName);
			data.count();
		}

		PhaseData<Long, Node> pd;
		if (outputPath != null)
			pd = new PhaseData<>(phaseName, data, outputPath);
		else
			pd = new PhaseData<>(phaseName, data);

		if (this.data.isEmpty()) {
			this.inputRDD = pd;
		} else {
			if (this.data.peek().getOutputPath() == null) {
				// If there is no plan to output this phase result, remove its reference
				PhaseData<Long, Node> d = this.data.pop();
				d.getData().unpersist(false);
			}
		}
		this.data.push(pd);
	}

	/**
	 * Returns the first RDD read by the system.
	 * @return The first RDD read by the system
	 */
	public JavaPairRDD<Long, Node> getStartingRDD() {
		return this.inputRDD.getData();
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
	 * @param config The configuration used by this execution
	 * @param outputPath The output path of the resulting RDD
	 * @param tmpPath The base path for the phase temporary results
	 * @param memoryAvailable The memory available for RDD storage purposes, used for split estimation
	 */
	public Data(Config config, String outputPath, String tmpPath, long memoryAvailable) {
		this.tmpOutput = tmpPath;
		this.output = outputPath;
		this.finalOutputSet = false;
		this.data = new Stack<>();
		this.inputRDD = null;
		this.outputRDD = null;
		this.config = config;
		this.splitStrategy = new PhaseSplitStrategy(config.getK(), memoryAvailable, config.getSplitMemoryConstant());
	}

}

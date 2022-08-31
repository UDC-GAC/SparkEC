package es.udc.gac.sparkec.preprocess;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;

import es.udc.gac.sparkec.Config;
import es.udc.gac.sparkec.Data;
import es.udc.gac.sparkec.Phase;
import es.udc.gac.sparkec.node.Node;
import scala.Tuple2;

/**
 * This Phase will read the input dataset, and filter the invalid reads.
 */
public class PreProcess implements Phase {
	private static Logger logger = LogManager.getLogger();

	/**
	 * The name of the phase.
	 */
	private final String phaseName = "00-preprocess";

	/**
	 * The first task to be run by this phase.
	 */
	private PreProcessTransform task1;

	/**
	 * The second task to be run by this phase.
	 */
	private PreProcessCheck task2;

	/**
	 * Whether this phase should generate a temporary output.
	 */
	private final boolean output;

	/**
	 * The input path where the input dataset is located. It might be either HDFS or a
	 * local file path.
	 */
	private final String inputPath;

	@Override
	public void runPhase(Data data) {

		JavaPairRDD<String, Node> raw;
		raw = task1.run(inputPath);

		raw.partitionBy(new HashPartitioner(raw.getNumPartitions()));

		Tuple2<JavaPairRDD<Long, String>, JavaPairRDD<Long, Node>> processed;
		processed = this.task2.run(raw);
		if (output)
			data.addOutputData(phaseName, processed._2, phaseName);
		else
			data.addTmpData(phaseName, processed._2);

		data.setMapping(processed._1);

	}

	@Override
	public String getPhaseName() {
		return phaseName;
	}

	@Override
	public void printStats() {
		logger.info(String.format("\t\t%d reads good", task2.getReads_good()));
		logger.info(String.format("\t\t%d reads_poly", task2.getReads_poly()));
		logger.info(String.format("\t\t%d reads_skipped", task2.getReads_skipped()));
		logger.info(String.format("\t\t%d reads_long", task2.getReads_long()));
		logger.info(String.format("\t\t%d reads_total",
				task2.getReads_good() + task2.getReads_poly() + task2.getReads_skipped()));
	}

	/**
	 * Default constructor for the PreProcess phase.
	 * @param config The configuration used by this execution
	 * @param inputPath The path of the input dataset
	 */
	public PreProcess(Config config, String inputPath) {

		output = config.isOutputPreprocess();
		this.inputPath = inputPath;

		this.task1 = new PreProcessTransform(config.getInputType(), config.getJavaSparkContext(),
				config.getJavaSparkContext().sc().longAccumulator(),
				config.getJavaSparkContext().sc().longAccumulator());

		this.task2 = new PreProcessCheck(config.getK(), config.isEnablePinchCorrect(), config.getShaveIgnore(), config.getJavaSparkContext());
	}

}

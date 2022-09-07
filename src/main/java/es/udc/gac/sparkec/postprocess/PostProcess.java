package es.udc.gac.sparkec.postprocess;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import es.udc.gac.sparkec.Config;
import es.udc.gac.sparkec.Data;
import es.udc.gac.sparkec.Phase;
import es.udc.gac.sparkec.node.Node;

/**
 * This Phase will output global stats about the corrected nodes, and the resulting dataset.
 */
public class PostProcess implements Phase {
	private static Logger logger = LogManager.getLogger();

	/**
	 * The name of the phase.
	 */
	private final String phaseName = "05-postprocess";

	/**
	 * The first task of PostProcess.
	 */
	private final Merge task1;

	/**
	 * The second task of PostProcess.
	 */
	private final Convert task2;

	/**
	 * Merge ignored reads from input. If false, task1 is not executed.
	 */
	private boolean mergeIgnore;

	/**
	 * Default constructor for PostProcess.
	 * @param c The Config for this execution.
	 */
	public PostProcess(Config c) {
		mergeIgnore = false;

		if (c.getShaveIgnore())
			mergeIgnore = c.getMergeIgnore();

		this.task1 = new Merge(c.getJavaSparkContext());
		this.task2 = new Convert(c.getJavaSparkContext());
	}

	@Override
	public void runPhase(Data data) {
		JavaPairRDD<Long, Node> in = data.getLatestData();
		if (mergeIgnore) {
			in = task1.run(in, data.getStartingRDD());
		}

		JavaRDD<String> out = task2.run(in, data.getMapping());
		data.addTmpData(phaseName, null);
		data.setOutputRDD(out);
	}

	@Override
	public String getPhaseName() {
		return this.phaseName;
	}

	@Override
	public void printStats() {
		if (mergeIgnore) {
			logger.info(String.format("\t\t%d readsIGN", task1.getReadsIgn()));
			logger.info(String.format("\t\t%d readsEC", task1.getReadsEC()));
			logger.info(String.format("\t\t%d readsFail", task1.getReadsFail()));
		}

		logger.info(String.format("\t\t%d uniqueReads", task2.getUniqueReads()));
		logger.info(String.format("\t\t%d outputReads", task2.getOutputReads()));
	}
}

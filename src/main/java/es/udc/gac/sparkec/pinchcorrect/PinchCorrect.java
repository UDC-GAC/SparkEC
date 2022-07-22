package es.udc.gac.sparkec.pinchcorrect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import es.udc.gac.sparkec.Config;
import es.udc.gac.sparkec.Data;
import es.udc.gac.sparkec.Phase;
import es.udc.gac.sparkec.node.Node;

/**
 * This Phase will take care of applying corrections using only the data under the kmers.
 */
public class PinchCorrect implements Phase {
	private static Logger logger = LogManager.getLogger();

	/**
	 * The name of the phase.
	 */
	private final String phaseName = "01-pinchcorrect";
	
	/**
	 * The first task of the phase.
	 */
	private final PinchCorrectRecommendation task1;
	
	/**
	 * The second task of the phase.
	 */
	private final PinchCorrectDecission task2;
	
	/**
	 * The number of passes to make of PinchCorrect.
	 */
	private final int attempts;
	
	/**
	 * Whether this phase should generate a temporary output.
	 */
	private final boolean output;

	@Override
	public void runPhase(Data data) {

		JavaPairRDD<Long, Node> input;
		input = data.getLatestData();

		JavaPairRDD<Long, Node> result = input;

		JavaPairRDD<Long, PinchCorrectSingleRecommendationData> tmp = null;
		for (int i = 0; i < attempts; i++) {

			tmp = task1.run(result, data.getSplitStrategy());
			result = task2.run(tmp, result);

		}

		input = result;

		if (output)
			data.addOutputData(phaseName, input, phaseName);
		else
			data.addTmpData(phaseName, input);

	}

	@Override
	public String getPhaseName() {
		return this.phaseName;
	}

	@Override
	public void printStats() {
		logger.info(String.format("\t\t%s ignoreds", task1.getIgnoredKMer()));
		logger.info(String.format("\t\t%s invalids", task1.getInvalidKMer()));
		logger.info(String.format("\t\t%s fix_chars (task1)", task1.getFix_char()));
		logger.info(String.format("\t\t%s fix_chars (task2)", task2.getFixChar()));
		logger.info(String.format("\t\t%s skip_chars", task2.getSkipChar()));
		logger.info(String.format("\t\t%s fix_reads", task2.getFixReads()));
		logger.info(String.format("\t\t%s total_decissions", task2.getTotales()));

	}

	public PinchCorrect(Config config) {
		this.attempts = config.getNumPinchCorrectAttempts();
		this.output = config.isOutputPinchCorrect();
		this.task1 = new PinchCorrectRecommendation(config.getK(), config.getJavaSparkContext());
		this.task2 = new PinchCorrectDecission(config.getK(), config.getJavaSparkContext());
		if (config.isKryoEnabled()) {
			config.getJavaSparkContext().sc().conf()
					.registerKryoClasses(
						new Class[] { 
							ReadInfo.class, 
							PinchCorrectSingleRecommendationData.class 
						});
		}
	}

}

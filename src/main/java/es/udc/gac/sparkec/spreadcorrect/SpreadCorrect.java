package es.udc.gac.sparkec.spreadcorrect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import es.udc.gac.sparkec.Config;
import es.udc.gac.sparkec.Data;
import es.udc.gac.sparkec.Phase;
import es.udc.gac.sparkec.node.Node;

/**
 * This phase will attempt to apply corrections, based on the context of each kmer after being aligned.
 */
public class SpreadCorrect implements Phase {
	private static Logger logger = LogManager.getLogger();

	/**
	 * First task to be run by SpreadCorrect.
	 */
	private final SpreadCorrectRecommend task1;
	
	/**
	 * Second task to be run by SpreadCorrect.
	 */
	private final SpreadCorrectDecission task2;
	
	/**
	 * Number of passes to make of SpreadCorrect.
	 */
	private final int attempts;
	
	/**
	 * The name of the phase.
	 */
	private final String phaseName = "03-spreadcorrect";
	
	/**
	 * Whether this phase should generate a temporary output.
	 */
	private final boolean output;

	@Override
	public void runPhase(Data data) {
		JavaPairRDD<Long, Node> result;
		result = data.getLatestData();
		JavaPairRDD<Long, Iterable<Recommendation>> recommendResult = null;

		for (int i = 0; i < attempts; i++) {
			recommendResult = task1.run(result, data.getSplitStrategy());
			result = task2.run(recommendResult, result);
		}
		if (attempts > 0) {
			if (output)
				data.addOutputData(phaseName, result, phaseName);
			else
				data.addTmpData(phaseName, result);
		}

	}

	@Override
	public String getPhaseName() {
		return phaseName;
	}

	@Override
	public void printStats() {
		logger.info(String.format("\t\t%d confirms (Recommend)", task1.getConfirms()));
		logger.info(String.format("\t\t%d fixChars (Recommend)", task1.getFixChars()));
		logger.info(String.format("\t\t%d correcteds (Decission)", task2.getCorrecteds()));
		logger.info(String.format("\t\t%d totals (Decission)", task2.getTotal()));
		logger.info(String.format("\t\t%d conflicts (Decission)", task2.getConflicts()));
		logger.info(String.format("\t\t%d fixChars (Decission)", task2.getFixChar()));
	}

	/**
	 * Default constructor for SpreadCorrect.
	 * @param c The configuration for this execution
	 */
	public SpreadCorrect(Config c) {
		this.output = c.isOutputSpreadCorrect();
		this.attempts = c.getNumSpreadCorrectAttempts();
		task1 = new SpreadCorrectRecommend(c.getJavaSparkContext(), c.getK(), c.getArm(), c.getHeight(), c.getScheme(),
				c.getStackMax(), c.getStackMin());

		task2 = new SpreadCorrectDecission(c.getJavaSparkContext());

		if (c.isKryoEnabled()) {
			c.getJavaSparkContext().sc().conf()
					.registerKryoClasses(new Class[] { Recommendation.class, SpreadCorrectKmerReadData.class });
		}
	}

}

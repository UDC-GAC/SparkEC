package es.udc.gac.sparkec.uniquekmer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import es.udc.gac.sparkec.Config;
import es.udc.gac.sparkec.Data;
import es.udc.gac.sparkec.Phase;
import es.udc.gac.sparkec.node.Node;

/**
 * This Phase will find the number of unique kmers of each sequence, and tag the sequences accordingly.
 */
public class UniqueKmerFilter implements Phase {
	private static Logger logger = LogManager.getLogger();

	/**
	 * The first task that will be run by this phase.
	 */
	private final CountKmers task1;
	
	/**
	 * The second task that will be run by this phase.
	 */
	private final TagReads task2;

	/**
	 * The name of this phase.
	 */
	private final String phaseName = "04-uniquekmerfilter";

	/**
	 * Whether this phase should generate a temporary output.
	 */
	private final boolean output;

	/**
	 * Default constructor for the UniqueKmerFilter.
	 * @param c The config being used
	 */
	public UniqueKmerFilter(Config c) {
		output = c.isOutputUniqueKmerFilter();
		task1 = new CountKmers(c.getK(), c.getJavaSparkContext());
		task2 = new TagReads(c.getJavaSparkContext());
	}

	@Override
	public void runPhase(Data data) {
		JavaPairRDD<Long, Node> in;

		JavaPairRDD<Long, Integer> tmp;
		JavaPairRDD<Long, Node> result;

		in = data.getLatestData();

		tmp = task1.run(in, data.getSplitStrategy());
		result = task2.run(tmp, in);

		if (output)
			data.addOutputData(phaseName, result, phaseName);
		else
			data.addTmpData(phaseName, result);
	}

	@Override
	public String getPhaseName() {
		return phaseName;
	}

	@Override
	public void printStats() {
		logger.info(String.format("\t\t%d unique_reads", task2.getUniqueReads()));
	}

}

package es.udc.gac.sparkec.largekmer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import es.udc.gac.sparkec.Config;
import es.udc.gac.sparkec.Data;
import es.udc.gac.sparkec.Phase;
import es.udc.gac.sparkec.node.Node;

/**
 * This Phase will generate ignore data to be used by the correction phases in order to speed up their 
 * computations.
 */
public class LargeKmerFilter implements Phase {
	private static Logger logger = LogManager.getLogger();

	/**
	 * The name of this Phase.
	 */
	private final String phaseName = "02-largekmerfilter";
	
	/**
	 * The first task of LargeKmerFilter.
	 */
	private final CountKmers task1;
	
	/**
	 * The second task of LargeKmerFilter.
	 */
	private final TagReads task2;
	
	/**
	 * Whether this phase should generate a temporary output.
	 */
	private final boolean output;

	@Override
	public void runPhase(Data data) {

		JavaPairRDD<Long, Node> in;
		in = data.getLatestData();

		JavaPairRDD<Long, KmerCount> tmp;
		tmp = task1.run(in, data.getSplitStrategy());

		JavaPairRDD<Long, Node> result;
		result = task2.run(tmp);
		
		if (output) {
			data.addOutputData(phaseName, result, phaseName);
		} else {
			data.addTmpData(phaseName, result);
		}

	}

	@Override
	public String getPhaseName() {
		return phaseName;
	}

	@Override
	public void printStats() {
		logger.info(String.format("\t\t%d hkmers", task1.getHkmer()));
		logger.info(String.format("\t\t%d lkmers", task1.getLkmer()));
	}

	/**
	 * Default constructor for LargeKmerFilter.
	 * @param c The Config being used at this execution
	 */
	public LargeKmerFilter(Config c) {
		this.output = c.isOutputLargeKmerFilter();
		task1 = new CountKmers(c.getK(), c.getFilter_P(), c.getFilter_S(),
				c.getJavaSparkContext(), c.getStackMax(), c.getStackMin());

		task2 = new TagReads(c.getJavaSparkContext(), c.getK());
		
		if (c.isKryoEnabled()) {
			c.getJavaSparkContext().sc().conf().registerKryoClasses(new Class[] {
				KmerIgnoreData.class,
				KmerCount.class
			});
		}
	}

}

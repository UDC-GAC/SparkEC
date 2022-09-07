package es.udc.gac.sparkec.split;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <p>
 * Base implementation of an SplitStrategy
 * 
 */
public class PhaseSplitStrategy implements IPhaseSplitStrategy {


	private static final long serialVersionUID = 1L;

	private static Logger logger = LogManager.getLogger();

	/**
	 * Internal coefficient to estimate the needed memory for each kmer
	 */
	private final float KMER_MEMORY_COEF;

	/**
	 * The base number of splits needed to handle each phase that needs to
	 * emmit kmers
	 */
	private double baseSplits = -1;

	/**
	 * The k currently used by the correction algorithm
	 */
	private final int k;

	/**
	 * The sum of total available memory for storage purposes among all
	 * Spark executors
	 */
	private final long availableMemory;

	@Override
	public int computeItemSplitIndex(Object item, int maxSplits, int prime) {
		int aux = item.hashCode();
		if (aux < 0) {
			aux *= -1;
		}
		return (aux % prime) % maxSplits;
	}

	@Override
	public int computeSplitPrime(int baseCuts, int partitions) {
		if (baseCuts < 2) {
			logger.info("Skipping split prime estimation (no splits specified for this phase).");
			return 1;
		}
		if ((partitions / 10) < baseCuts) {
			logger.warn("The split count specified (" + baseCuts + ") is too high for this number of partitions (" + partitions + ") .");
			logger.warn("Consider increasing the number of partitions. This attempt will probably generate empty Spark tasks.");
			return 1;
		}

		for (int i = baseCuts; i < (partitions / 10); i++) {
			if ((partitions % i) != 0) {
				return i;
			}
		}
		logger.warn("No suitable prime number was found to handle this number of partitions (" + partitions + ")! ");
		logger.warn(
				"Either repartition the initial dataset or lower your required number of cuts. This execution will generate empty Spark tasks!");
		return baseCuts;
	}


	@Override
	public int getSplits(int splitsMultiplier) {
		if (this.baseSplits == -1) {
			throw new UninitializedPhaseSplitStrategyException();
		}

		int splits = (int)Math.ceil(this.baseSplits * splitsMultiplier);
		if (splits < 1) {
			return 1;
		}
		return splits;
	}

	@Override
	public void initialize(int seqLen, long nodeCount) {
		double baseKmerMemory = ((seqLen - k) * k) * nodeCount * KMER_MEMORY_COEF;
		this.baseSplits = baseKmerMemory / availableMemory;
		logger.info("baseKmerMemory = "+(long)baseKmerMemory);
		logger.info("baseSplits = "+baseSplits);
	}

	/**
	 * Default constructor for this implementation of the PhaseSplitStrategy
	 * @param k The k being currently used by the algorithm
	 * @param availableMemory The total available memory for the RDD storage of this execution
	 * @param memoryConstant The constant used for the estimation of the memory needed for the execution
	 *  among all the executor nodes.
	 */
	public PhaseSplitStrategy(int k, long availableMemory, float memoryConstant) {
		this.k = k;
		this.availableMemory = availableMemory;
		this.KMER_MEMORY_COEF = memoryConstant;
	}
}

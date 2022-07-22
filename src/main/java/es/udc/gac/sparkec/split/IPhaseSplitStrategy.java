package es.udc.gac.sparkec.split;

import java.io.Serializable;

/**
 * <p>
 * Base interface for the different Split Strategies.
 * 
 * <p>
 * The goal of this class is to act as a base class to provide strategies to compute the distribution of the splits
 * of those phases that support it. The splits allow the phases to keep their memory usage under a threshold. With
 * this improvement, the computation can fit into memory with hardware configurations that otherwise would use disk.
 * 
 */
public interface IPhaseSplitStrategy extends Serializable {

	/**
	 * Auxiliary method to compute the current split of a given object. In order to compute it,
	 * the hashCode of the object will be called.
	 * @param item The item whose split is being computed
	 * @param maxSplits Total number of splits
	 * @param prime Prime number with the total number of splits. Used in order to keep the HashPartitioner
	 * partitions distribution uniform.
	 * @return The current split of the item
	 */
	int computeItemSplitIndex(Object item, int maxSplits, int prime);
	
	/**
	 * Computes a prime number with the number of partitions of the input. This prime must be used inside the
	 * computeItemSplitIndex calls.
	 * @param baseCuts The number of splits
	 * @param partitions The number of partitions
	 * @return The prime number with the number of partitions
	 */
	int computeSplitPrime(int baseCuts, int partitions);
	
	/**
	 * Returns the number of splits for a phase.
	 * @param splitsMultiplier  A multiplier applied to the current global number of splits defined. It allows
	 * to use more splits in a given phase, if that phase is expected to use more memory
	 * @return The number of splits for the phase
	 */
	int getSplits(int splitsMultiplier);

	/**
	 * Initializes the SplitStrategy for a given dataset.
	 * @param seqLen The average sequence length for this dataset
	 * @param nodeCount The number of sequences of this dataset
	 */
	void initialize(int seqLen, long nodeCount);

}
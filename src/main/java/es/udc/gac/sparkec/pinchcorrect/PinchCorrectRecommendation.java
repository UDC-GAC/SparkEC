package es.udc.gac.sparkec.pinchcorrect;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import es.udc.gac.sparkec.node.EncodingUtils;
import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.sequence.IDNASequence;
import es.udc.gac.sparkec.sequence.UnableToSkipMiddleBase;
import es.udc.gac.sparkec.split.IPhaseSplitStrategy;
import scala.Tuple2;

/**
 * SubPhase of the PinchCorrect Phase. This SubPhase will emmit recommendations to be applied at each Node.
 */
public class PinchCorrectRecommendation implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LogManager.getLogger();
	
	/**
	 * Maximum ratio between loser and winner bases quality values.
	 */
	private static final float QRATIO_LOSER = 0.25f;
	
	/**
	 * Minimum quality value for the winner base.
	 */
	private static final short QSUM_WINNER = 60;
	
	/**
	 * Minimum good quality values required for the winner base.
	 */
	private static final short QV_GOOD_CNT = 1;
	
	/**
	 * Minimum number of reads for a kmer to emmit recommendations.
	 */
	private static final short READ_MIN = 6;
	
	/**
	 * Split multiplier for this SubPhase.
	 */
	private static final int SPLIT_MULTIPLIER = 1;

	/**
	 * The k being used by the current execution.
	 */
	private final int k;

	/**
	 * Number of change recommendations made.
	 */
	private final LongAccumulator fix_char;

	/**
	 * Number of k-mers that were ignored because they were flagged to do so in the
	 * IGN field of the node.
	 */
	private final LongAccumulator ignoredKMer;

	/**
	 * Number of k-mers filtered because of containing a 'N'.
	 */
	private final LongAccumulator invalidKMer;

	/**
	 * Returns the value of the FixChar accumulator.
	 * @return The value of the FixChar accumulator
	 */
	public long getFix_char() {
		return fix_char.value();
	}

	/**
	 * Returns the value of the IgnoredKmer accumulator.
	 * @return The value of the IgnoredKmer accumulator
	 */
	public long getIgnoredKMer() {
		return ignoredKMer.value();
	}

	/**
	 * Returns the value of the InvalidKmer accumulator.
	 * @return The value of the InvalidKmer accumulator
	 */
	public long getInvalidKMer() {
		return invalidKMer.value();
	}

	/**
	 * Default constructor for PinchCorrectRecommendation.
	 * @param k The k being used by the current execution
	 * @param jsc The JavaSparkContext
	 */
	public PinchCorrectRecommendation(int k, JavaSparkContext jsc) {
		this.k = k;
		this.fix_char = jsc.sc().longAccumulator();
		this.ignoredKMer = jsc.sc().longAccumulator();
		this.invalidKMer = jsc.sc().longAccumulator();
	}

	/**
	 * Emit a list of kmer found in each (inverted depending on alphabetic order).
	 * Does not generate kmers ignored by IgnP, neither who have any 'N' base.
	 * 
	 * @param line        The node whose kmers are about to be generated
	 * @param splitIndex  The current split being computed
	 * @param splitCount  The total number of splits being computed
	 * @param k           Size of the kmer
	 * @param ignoredKmer Accumulator to report back the number of ignoredKmers
	 * @param invalidKmer Accumulator to report back the number of invalidKmers
	 * @param splitPrime The split prime used to distribute the splits
	 * @param splitStrategy The split strategy currently being used
	 * @return The set of kmers generated, with information of the read who referred
	 *         them
	 */
	public static Iterable<Tuple2<IDNASequence, ReadInfo>> generateKmers(Tuple2<Long, Node> line, int splitIndex,
			int splitCount, int k, LongAccumulator ignoredKmer, LongAccumulator invalidKmer, int splitPrime,
			IPhaseSplitStrategy splitStrategy) {
		List<Tuple2<IDNASequence, ReadInfo>> result = new LinkedList<>();

		Node node = line._2;

		IDNASequence seq = node.getSeq();
		byte[] qv = node.getQv();

		String nodeIGN = node.getIgnP();
		int end = seq.length() - (k + 1) + 1;
		for (int i = 0; i < end; i++) {

			if (nodeIGN != null && nodeIGN.charAt(i) == '1') {
				if (ignoredKmer != null) {
					ignoredKmer.add(1);
				}

				continue;
			}

			IDNASequence window_f;
			try {
				window_f = seq.getSubSequenceWithoutMiddleBase(i, i + k);
			} catch (UnableToSkipMiddleBase e) {
				throw new RuntimeException(e);
			}
			IDNASequence window_r = window_f.rcSequence();

			boolean isInvalidKmer = window_f.containsBase((byte) 'N');

			if (isInvalidKmer) {
				if (invalidKmer != null) {
					invalidKmer.add(1L);
				}
				continue;
			}

			int middle_pos = i + (k / 2);

			boolean reversed = window_f.compareTo(window_r) > 0;

			if ((!reversed) && (splitStrategy.computeItemSplitIndex(window_f, splitCount, splitPrime) == splitIndex)) {
				result.add(new Tuple2<IDNASequence, ReadInfo>(window_f, new ReadInfo(line._1, true, (short) middle_pos,
						(char) seq.getValue(middle_pos), (char) qv[middle_pos], (short) seq.length())));

			} else if ((reversed)
					&& (splitStrategy.computeItemSplitIndex(window_r, splitCount, splitPrime) == splitIndex)) {
				result.add(new Tuple2<IDNASequence, ReadInfo>(window_r, new ReadInfo(line._1, false, (short) middle_pos,
						(char)IDNASequence.getAlternateBase(seq.getValue(middle_pos)), (char) qv[middle_pos], (short) seq.length())));
			}
		}

		return result;
	}

	/**
	 * Generate a list of recommendations for a given kmer. In order to do this, we
	 * will attempt to find out a winner base (by comparing the quality values of
	 * each different base). Once a winner base is found (and if it has an enough
	 * quality), we make a recommendation to each aparition of each base that has a
	 * quality lower than a minimum, and also lower than a fraction of the winner
	 * quality (or if the base was an 'N').
	 * 
	 * @param kmer    The kmer data, with a list of the reads that had that kmer
	 * @param fixChar Accumulator to report back the number of characters
	 *                recommended to fix
	 * @return A list with the recommendations. The key is the nodeID where to apply
	 *         it, the value is the recommendation itself
	 */
	public static Iterable<Tuple2<Long, PinchCorrectSingleRecommendationData>> generateRecommendations(
			Tuple2<IDNASequence, Iterable<ReadInfo>> kmer, LongAccumulator fixChar) {
		Iterator<ReadInfo> aux = kmer._2.iterator();

		List<Tuple2<Long, PinchCorrectSingleRecommendationData>> result = new LinkedList<>();

		long readSize = 0;

		while (aux.hasNext()) {
			aux.next();
			readSize++;
		}

		if (readSize < READ_MIN) {
			return result;
		}

		// This list keeps tracks of the qualities for each base
		// [0]=A, [1]=T, [2]=C, [3]=G
		int[] qv_sum = new int[4];
		int[] qv_good_cnt = new int[4];
		boolean[] recommends = new boolean[4];

		int baseCnts = 0;

		for (int base = 0; base < qv_sum.length; base++) {
			qv_sum[base] = 0;
			qv_good_cnt[base] = 0;
			recommends[base] = false;
		}

		aux = kmer._2.iterator();
		// summary (A, T, C, G) of each read position
		while (aux.hasNext()) {

			ReadInfo readitem = aux.next();

			int quality_value = readitem.getQv();
			int base = EncodingUtils.char2idx((char) readitem.getBase());

			if (base != EncodingUtils.char2idx('N')) {

				baseCnts++;

				qv_sum[base] += quality_value;

				if (quality_value >= EncodingUtils.QV_GOOD) {

					qv_good_cnt[base]++;
				}
			}
		}

		// skip this position since its has no enough bases
		if (baseCnts < READ_MIN) {
			return result;
		}

		// find out the winner of the position
		int winner_base = 0;

		if (qv_sum[2] > qv_sum[winner_base]) {
			winner_base = 2;
		}
		if (qv_sum[3] > qv_sum[winner_base]) {
			winner_base = 3;
		}
		if (qv_sum[1] > qv_sum[winner_base]) {
			winner_base = 1;
		}

		// winner's qv sum must >= QSUM_WINNER_P
		if (qv_sum[winner_base] < QSUM_WINNER) {

			return result;
		} // Sum of qualities of winner base must be bigger than a minimum

		// make a replace recommendation for all loser bases iff
		// 1) loser's good qv count <= QV_GOOD_CNT_P
		// 2) loser's qv sum < QRATIO_LOSER_P times of the winner

		for (int base = 0; base < recommends.length; base++) {
			if (base != winner_base) {

				if (qv_good_cnt[base] <= QV_GOOD_CNT
						&& (qv_sum[base] < QRATIO_LOSER * qv_sum[winner_base])) {
					recommends[base] = true;

				}
			}
		} // A recommendation is made if it's quality was quite lower than the winner one

		aux = kmer._2.iterator();
		// make recommendation of each read position
		for (int i = 0; i < readSize; i++) {
			ReadInfo readitem = aux.next();

			int base = EncodingUtils.char2idx((char) readitem.getBase());

			if (base != winner_base) { // If the base was different (recommend modify):

				if (base == EncodingUtils.char2idx('N') || recommends[base]) { // If recommendable or 'N'
					char chr = EncodingUtils.idx2char(winner_base);

					// rc the base char if reversed
					if (!readitem.isDir()) {
						chr = (char)IDNASequence.getAlternateBase((byte)chr);
					}

					// pos from mapper is always dir f
					result.add(new Tuple2<>(readitem.getId(),
							new PinchCorrectSingleRecommendationData((byte) chr, readitem.getPos())));
					fixChar.add(1);
				}
			}
		}

		return result;
	}

	/**
	 * Computes a single split of PinchCorrectRecommendation. It emmits the Recommendations associated
	 * with a single split.
	 * @param in The input dataset
	 * @param splitIndex The split to compute
	 * @param splitCount The number of splits being computed
	 * @param splitPrime The split prime to use to distribute the values
	 * @param splitStrategy The split strategy currently being used
	 * @return The recommendations of this single split
	 */
	private JavaPairRDD<Long, PinchCorrectSingleRecommendationData> computeSplit(JavaPairRDD<Long, Node> in,
			int splitIndex, int splitCount, int splitPrime, IPhaseSplitStrategy splitStrategy) {

		JavaPairRDD<IDNASequence, ReadInfo> tmp;
		tmp = in.flatMapToPair(line -> generateKmers(line, splitIndex, splitCount, k, this.ignoredKMer,
				this.invalidKMer, splitPrime, splitStrategy).iterator());

		JavaPairRDD<IDNASequence, Iterable<ReadInfo>> reads;
		reads = tmp.groupByKey(new HashPartitioner(in.getNumPartitions()));

		JavaPairRDD<Long, PinchCorrectSingleRecommendationData> recommendations;
		recommendations = reads.flatMapToPair(element -> generateRecommendations(element, fix_char).iterator());

		return recommendations;

	}

	/**
	 * Runs this SubPhase.
	 * @param in The input dataset
	 * @param splitStrategy The split strategy being used
	 * @return The recommendations for each Node
	 */
	public JavaPairRDD<Long, PinchCorrectSingleRecommendationData> run(JavaPairRDD<Long, Node> in,
			IPhaseSplitStrategy splitStrategy) {
		int splitCount = splitStrategy.getSplits(SPLIT_MULTIPLIER);
		logger.info("Computing PinchCorrectRecommendation using " + splitCount + " splits");

		int splitPrime = splitStrategy.computeSplitPrime(splitCount, in.getNumPartitions());

		JavaPairRDD<Long, PinchCorrectSingleRecommendationData> output = this.computeSplit(in, 0, splitCount,
				splitPrime, splitStrategy);

		for (int i = 1; i < splitCount; i++) {
			output.count();
			JavaPairRDD<Long, PinchCorrectSingleRecommendationData> split = this.computeSplit(in, i, splitCount,
					splitPrime, splitStrategy);
			output = output.union(split);
		}

		return output;
	}
}

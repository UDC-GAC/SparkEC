package es.udc.gac.sparkec.spreadcorrect;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
import es.udc.gac.sparkec.split.IPhaseSplitStrategy;
import scala.Tuple2;

/**
 * SubPhase of the SpreadCorrect phase. This SubPhase takes care of generating all the node recommendations
 * that will be used to make decisions for each node.
 */
public class SpreadCorrectRecommend implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LogManager.getLogger();

	/**
	 * Maximum quality value to make a replace recommendation
	 */
	private static final short QSUM_REPLACE = 60;
	
	/**
	 * Minimum quality value to make a protect recommendation
	 */
	private static final short QSUM_PROTECT = 90;
	
	/**
	 * Maximum ratio between loser and winner bases to make a replace recommendation.
	 */
	private static final float QRATIO_LOSER = 0.25f;
	
	/**
	 * Minimum number of good QVs to identify a base as a winner base. 
	 */
	private static final short QV_GOOD_CNT = 3;
	
	/**
	 * Minimum quality value of a winner base.
	 */
	private static final short QSUM_WINNER = 40;
	
	/**
	 * Whether recommendations under the kmer itself should be emmited.
	 */
	private static final boolean SKIP_UNDER = true;
	
	/**
	 * Minimum number of bases of an arm to start emmiting recommendations for it.
	 */
	private static final short BASE_MIN = 1;
	
	/**
	 * Multiplier applied to the number of base splits, in order to get the final number of splits.
	 * This multiplier allows to apply more splits in this phase, since it requires more memory for
	 * each kmer.
	 */
	private static final int SPLITS_MULTIPLIER = 4;

	/**
	 * The k used by this execution.
	 */
	private final int k;
	
	/**
	 * The arm size.
	 */
	private final int arm;
	
	/**
	 * The height.
	 */
	private final int height;
	
	/**
	 * The arm scheme to use.
	 */
	private final String scheme;

	/**
	 * The maximum number of reads to make recommendations.
	 */
	private final int stackMax;
	
	/**
	 * The minimum number of reads to make recommendations.
	 */
	private final int stackMin;

	/**
	 * Different accumulator.
	 */
	private final LongAccumulator differents;
	
	/**
	 * Equals accumulator.
	 */
	private final LongAccumulator equals;

	/**
	 * Confirms accumulator.
	 */
	private final LongAccumulator confirms;
	
	/**
	 * FixChars accumulator.
	 */
	private final LongAccumulator fixChars;

	/**
	 * Get the value of the different accumulator.
	 * @return The value of the different accumulator
	 */
	public long getDifferents() {
		return differents.value();
	}

	/**
	 * Get the value of the equal accumulator.
	 * @return The value of the equal accumulator
	 */
	public long getEquals() {
		return equals.value();
	}

	/**
	 * Get the value of the confirm accumulator.
	 * @return The value of the confirm accumulator
	 */
	public long getConfirms() {
		return confirms.value();
	}

	/**
	 * Get the value of the FixChar accumulator.
	 * @return The value of the FixChar accumulator
	 */
	public long getFixChars() {
		return fixChars.value();
	}

	/**
	 * Default constructor for SpreadCorrectRecommend.
	 * @param jsc The JavaSparkContext
	 * @param k The k used by this execution
	 * @param arm The arm size
	 * @param height The height
	 * @param scheme The scheme of the arm
	 * @param stackMax The maximum number of nodes to make recommendations
	 * @param stackMin The minimum number of nodes to make recommendations
	 */
	public SpreadCorrectRecommend(JavaSparkContext jsc, int k, int arm, int height, String scheme, int stackMax,
			int stackMin) {
		this.k = k;
		this.arm = arm;
		this.height = height;
		this.scheme = scheme;
		this.stackMax = stackMax;
		this.stackMin = stackMin;
		this.differents = jsc.sc().longAccumulator();
		this.equals = jsc.sc().longAccumulator();
		this.confirms = jsc.sc().longAccumulator();
		this.fixChars = jsc.sc().longAccumulator();
	}

	/**
	 * Auxiliary method that will try to add recommendations from a list of reads of
	 * a given k-mer, and will insert them into a list of Recommendations. It can
	 * make change recommendations or protection recommendations, in a change
	 * recommendation the recommended base is sent, and in a protect recommendation,
	 * the recommended base is just a 'N'.
	 * 
	 * @param readlist   List containing all the reads of the given kmer.
	 * @param colbias    Current position to be processed.
	 * @param under_kmer If this position was under the aligned k-mer, or if it on a
	 *                   side.
	 * @param out_list   The list where the recommendations will be saved.
	 * @return If a base was chosen to make recommendations.
	 */
	private static boolean makeColEC(final Iterable<SpreadCorrectKmerReadData> readlist, final int colbias,
			final boolean under_kmer, List<Tuple2<Long, Recommendation>> out_list) {

		// [0]=A, [1]=T, [2]=C, [3]=G
		int[] qv_sum = new int[4];
		int[] qv_good_cnt = new int[4];
		boolean[] recommends = new boolean[4];

		for (int base = 0; base < qv_sum.length; base++) {
			qv_sum[base] = 0;
			qv_good_cnt[base] = 0;
			recommends[base] = false;
		}

		int baseCnts = 0;

		Iterator<SpreadCorrectKmerReadData> it = readlist.iterator();
		// summary (A, T, C, G) of each read position
		while (it.hasNext()) {
			SpreadCorrectKmerReadData readitem = it.next();

			int pos = readitem.getPos() + colbias - readitem.getOffset();

			if (pos < 0 || pos > readitem.getSeq().length() - 1) {
				continue;
			}

			int quality_value = readitem.getQv()[pos];
			int base = EncodingUtils.char2idx((char) readitem.getSeq().getValue(pos));

			if (base != EncodingUtils.char2idx('N')) {
				baseCnts++;

				qv_sum[base] += quality_value;

				if (quality_value >= EncodingUtils.QV_GOOD) {
					qv_good_cnt[base]++;
				}
			}
		}

		// skip this position since it has no enough bases
		if (baseCnts < BASE_MIN) {
			return false;
		}

		// find out the winner of the position
		int base_winner = 0;

		if (qv_sum[2] > qv_sum[base_winner]) {
			base_winner = 2;
		}
		if (qv_sum[3] > qv_sum[base_winner]) {
			base_winner = 3;
		}
		if (qv_sum[1] > qv_sum[base_winner]) {
			base_winner = 1;
		}

		// winner's qv sum must >= QSUM_WINNER_S
		if (qv_sum[base_winner] < QSUM_WINNER) {
			return false;
		}

		// the maximum value of non-winner bases
		int qv_sum_max_loser = 0;

		for (int base = 0; base < qv_sum.length; base++) {
			if (base != base_winner) {
				if (qv_sum_max_loser < qv_sum[base]) {
					qv_sum_max_loser = qv_sum[base];
				}
			}
		}

		// make a replace recommendation for all loser bases iff
		// 1) losers' qv sum ARE all <= QRATIO_LOSER_S times of the winner
		// 2) loser is not corrected, and its qv sum < QSUM_REPLACE
		if (qv_sum_max_loser <= QRATIO_LOSER * qv_sum[base_winner]) {
			for (int base = 0; base < recommends.length; base++) {
				if (base != base_winner) {
					if (qv_sum[base] != 0 && qv_sum[base] < QSUM_REPLACE) {
						recommends[base] = true;
					}
				}
			}
		}

		// make a protect recommendation for the winner iff
		// 1) loser's qv sum are ALL < QSUM_REPLACE
		// 2) winner's qv sum >= QSUM_REPLACE if under the kmer range
		// 3) winner's qv sum >= QSUM_PROTECT,
		// or winner's good qv count is at least QV_GOOD_CNT_S
		if (qv_sum_max_loser < QSUM_REPLACE) {
			if (!under_kmer || qv_sum[base_winner] >= QSUM_REPLACE) {
				if (qv_sum[base_winner] >= QSUM_PROTECT || qv_good_cnt[base_winner] >= QV_GOOD_CNT) {
					recommends[base_winner] = true;
				}
			}
		}

		it = readlist.iterator();
		// make recommendation of each read position
		while (it.hasNext()) {
			SpreadCorrectKmerReadData readitem = it.next();

			int pos = readitem.getPos() + colbias - readitem.getOffset();

			if (pos < 0 || pos > readitem.getSeq().length() - 1) {
				continue;
			}

			int base = EncodingUtils.char2idx((char) readitem.getSeq().getValue(pos));

			if (base == base_winner) {
				if (recommends[base]) {
					pos = pos + readitem.getOffset();

					if (readitem.isReversed()) {
						pos = readitem.getLength() - 1 - pos;
					}

					out_list.add(new Tuple2<>(readitem.getNodeId(), new Recommendation('N', pos)));
					// Se utiliza la N para marcar que se recomienda proteger

					// reporter.incrCounter("Brush", "confirm_char", 1);
				}
			} else {
				if (base == EncodingUtils.char2idx('N') || recommends[base]) {
					char chr = EncodingUtils.idx2char(base_winner);

					pos = pos + readitem.getOffset();

					if (readitem.isReversed()) {
						chr =  (char)IDNASequence.getAlternateBase((byte)chr);
						pos = readitem.getLength() - 1 - pos;
					}

					out_list.add(new Tuple2<>(readitem.getNodeId(), new Recommendation(chr, pos)));

					// reporter.incrCounter("Brush", "fix_char", 1);
				}
			}
		}

		// skip the rest positions because of a branch
		for (int base = 0; base < qv_sum.length; base++) {
			if (base != base_winner) {
				if (qv_sum[base] >= QSUM_REPLACE) {
					return true;
				}
			}
		}

		return false;
	}

	/**
	 * We will generate kmers for each node, tagging each kmer with data about the
	 * sequences around it.
	 * 
	 * @param data       The node for which the kmer is going to be generated
	 * @param k          The size of the kmer to generate
	 * @param arm        The arm
	 * @param scheme     The scheme
	 * @param height     The height
	 * @param splitIndex The current split currently being computed
	 * @param splitCount The number of splits being computed
	 * @param splitPrime The prime number used for ditribution of splits
	 * @param splitStrategy The split strategy currently being used
	 * @return List with the kmers generated, as well as information about the read
	 *         who generated them
	 */
	public static Iterable<Tuple2<IDNASequence, SpreadCorrectKmerReadData>> generateKmers(Tuple2<Long, Node> data,
			int k, int arm, String scheme, int height, int splitIndex, int splitCount, int splitPrime,
			IPhaseSplitStrategy splitStrategy) {

		Node node = data._2;
		List<Tuple2<IDNASequence, SpreadCorrectKmerReadData>> out = new LinkedList<>();

		Long nodeID = data._1;
		IDNASequence nodeSEQ = node.getSeq();
		byte[] nodeQV = EncodingUtils.qvSmooth(EncodingUtils.qvDeflate(node.getQv()));

		String nodeIGN = EncodingUtils.decodeIGN(node.getIgnF());

		// Here we use a Kmer for alignment
		int end = nodeSEQ.length() - k + 1;
		for (int i = 0; i < end; i++) {
			// ignore kmers

			if (nodeIGN != null && nodeIGN.charAt(i) == '1') {
				continue;
			}

			IDNASequence kmer_f = nodeSEQ.getSubSequence(i, i + k - 1);
			IDNASequence kmer_r = kmer_f.rcSequence();

			boolean invalidKmer = kmer_f.containsBase((byte) 'N');
			if (invalidKmer) {
				continue;
			}

			int wing_pos_left = 0;
			int wing_pos_right = nodeSEQ.length();

			if (arm != -1) {
				if (scheme != null && scheme.equals("CLA")) {
					if (i < arm) {
						wing_pos_left = Math.max(0, i - arm);
						wing_pos_right = Math.min(i + k + (2 * arm - i), // k + 2 * arm ???
								nodeSEQ.length());
					} else if (nodeSEQ.length() - (i + k) < arm) {
						wing_pos_right = Math.min(i + k + arm, nodeSEQ.length());
						wing_pos_left = Math.max(0, i - (2 * arm - (wing_pos_right - (i + k))));
					} else {
						wing_pos_left = i - arm;
						wing_pos_right = i + k + arm;
					}
				} else if (scheme != null && scheme.equals("ENV")) {
					/* for left */
					if (i < k + arm) {
						wing_pos_left = Math.max(0, i - arm);
					} else {
						wing_pos_left = k
								+ (int) Math.floor(1.0 * (i - (k + arm) + 1) * (end - k - 1) / (end - (k + arm) + 1));
					}

					/* for right */
					if (i < end - (k + arm)) {
						wing_pos_right = k + (int) Math.floor(1.0 * (i + 1) * (end - k - 1) / (end - (k + arm) + 1));
					} else {
						wing_pos_right = Math.min(i + k + arm, nodeSEQ.length());

					}
				} else if (scheme != null && scheme.equals("GNV")) {
					int PtA = arm + height;
					int PtD = arm;
					int PtB = arm + (int) Math.ceil(1.0 * end * (PtA - PtD) / end);
					int PtC = arm + (int) Math.ceil(1.0 * k * (PtA - PtD) / nodeSEQ.length());

					if (i < PtA + 1) {
						// wing_pos_left = 0;
					} else if (i >= PtA + 1 && i < k + PtB) {
						wing_pos_left = 0
								+ (int) Math.floor(1.0 * (i - (PtA + 1) + 1) * (k - 1) / ((k + PtB) - (PtA + 1)));
					} else {
						wing_pos_left = k
								+ (int) Math.floor(1.0 * (i - (k + PtB) + 1) * (end - k - 1) / (end - (k + PtB) + 1));
					}

					if (i < end - (k + PtC)) {
						wing_pos_right = k + (int) Math.floor(1.0 * (i + 1) * (end - k - 1) / (end - (k + PtC)));
					} else if (i >= end - (k + PtC) && i < end - arm - 1) {
						wing_pos_right = (end - 1) + (int) Math.floor(
								1.0 * (i - (end - (k + PtC)) + 1) * (k - 1) / ((end - arm - 1) - (end - (k + PtC))));
					} else {
						// wing_pos_right = nodeSEQ.length();
					}
				} else {
					wing_pos_left = Math.max(0, i - arm);
					wing_pos_right = Math.min(i + k + arm, nodeSEQ.length());
				}
			}

			boolean reversed = kmer_f.compareTo(kmer_r) > 0;

			if ((!reversed) && (splitStrategy.computeItemSplitIndex(kmer_f, splitCount, splitPrime) == splitIndex)) {
				SpreadCorrectKmerReadData tmp = new SpreadCorrectKmerReadData(nodeID, false, i,
						nodeSEQ.getSubSequence(wing_pos_left, wing_pos_right - 1), kmer_f,
						EncodingUtils.qvValueConvert(EncodingUtils.qvInflate(Arrays.copyOfRange(nodeQV, wing_pos_left, wing_pos_right)),
								false),
						wing_pos_left, nodeSEQ.length());

				out.add(new Tuple2<>(kmer_f.copy(), tmp));
			} else if ((reversed)
					&& (splitStrategy.computeItemSplitIndex(kmer_r, splitCount, splitPrime) == splitIndex)) {
				SpreadCorrectKmerReadData tmp = new SpreadCorrectKmerReadData(nodeID, true, end - i - 1,
						nodeSEQ.rcSequence().getSubSequence(wing_pos_left, wing_pos_right - 1, true), kmer_r,
						EncodingUtils.qvValueConvert(EncodingUtils.qvInflate(Arrays.copyOfRange(nodeQV, wing_pos_left, wing_pos_right)),
								true),
						nodeSEQ.length() - wing_pos_right, nodeSEQ.length());
				out.add(new Tuple2<>(kmer_r.copy(), tmp));
			}
		}

		return out;
	}

	/**
	 * Generate recommendations for each kmer and the node data attached to it.
	 * 
	 * @param data     The kmer and it's attached nodes data for each node that
	 *                 referenced it.
	 * @param stackMin The minimum number of reads that must reference this kmer in
	 *                 order to generate recommendations for him (or -1 to disable
	 *                 this feature)
	 * @param stackMax The maximum number of reads that must reference this kmer in
	 *                 order to generate recommendations for him (or -1 to disable
	 *                 this feature)
	 * @param k        The size of the kmer
	 * @param fixChars Accumulator to report back the number of characters fix
	 *                 recommendations
	 * @param confirms Accumulator to report back the number of confirms
	 * @return The list of the recommendations, with the NodeID as key, and the
	 *         recommendation data as value
	 */
	public static Iterable<Tuple2<Long, Recommendation>> generateRecommendations(
			Tuple2<IDNASequence, Iterable<SpreadCorrectKmerReadData>> data, int stackMin, int stackMax, int k,
			LongAccumulator fixChars, LongAccumulator confirms) {

		List<Tuple2<Long, Recommendation>> out = new LinkedList<>();

		Iterator<SpreadCorrectKmerReadData> it = data._2.iterator();

		int armLeft = -1;
		int armRight = -1;

		long readSize = 0;

		while (it.hasNext()) {
			SpreadCorrectKmerReadData tmp = it.next();

			armLeft = Math.max(armLeft, tmp.getARMLeft());
			armRight = Math.max(armRight, tmp.getARMRight(k));
			readSize++;
		}

		// skip large or small stack, -1 ignore
		if (((stackMax != -1 && readSize > stackMax) || (stackMin != -1 && readSize < stackMin))) {
			return out;
		}

		fixChars.add(1);
		confirms.add(armRight);

		// left range
		for (int j = armLeft - 1; j >= 0; j--) {

			boolean branch = false;
			branch = makeColEC(data._2, (-armLeft + j), false, out);
			if (branch) {
				break;
			}
		}

		// K range
		if (!SKIP_UNDER) {
			for (int j = 0; j < k; j++) {
				makeColEC(data._2, j, true, out);
			}
		}

		// right range
		for (int j = 0; j < armRight; j++) {
			boolean branch = false;
			branch = makeColEC(data._2, (j + k), false, out);
			if (branch) {
				break;
			}
		}

		return out;
	}

	/**
	 * Generates the recommendations of a single split.
	 * @param in The nodes whose recommendations are being made
	 * @param splitIndex The current split being generated
	 * @param splitCount The number of splits
	 * @param splitPrime The prime number used by the split strategy
	 * @param splitStrategy The split strategy being used
	 * @return The recommendations of this split
	 */
	public JavaPairRDD<Long, Iterable<Recommendation>> computeSplit(JavaPairRDD<Long, Node> in, int splitIndex,
			int splitCount, int splitPrime, IPhaseSplitStrategy splitStrategy) {

		JavaPairRDD<IDNASequence, SpreadCorrectKmerReadData> kmers;
		kmers = in.flatMapToPair(
				node -> generateKmers(node, k, arm, scheme, height, splitIndex, splitCount, splitPrime, splitStrategy)
						.iterator());

		JavaPairRDD<IDNASequence, Iterable<SpreadCorrectKmerReadData>> kmersGrouped;
		kmersGrouped = kmers.groupByKey(new HashPartitioner(in.getNumPartitions()));

		JavaPairRDD<Long, Recommendation> corrections;
		corrections = kmersGrouped.flatMapToPair(
				data -> generateRecommendations(data, stackMin, stackMax, k, fixChars, confirms).iterator());

		JavaPairRDD<Long, Iterable<Recommendation>> correctionsGrouped;
		correctionsGrouped = corrections.groupByKey(new HashPartitioner(in.getNumPartitions()));

		return correctionsGrouped;

	}

	/**
	 * Runs this SubPhase.
	 * @param in The nodes whose recommendations are being generated
	 * @param splitStrategy The split strategy being used
	 * @return The recommendations for SpreadCorrect
	 */
	public JavaPairRDD<Long, Iterable<Recommendation>> run(JavaPairRDD<Long, Node> in,
			IPhaseSplitStrategy splitStrategy) {

		int splitCount = splitStrategy.getSplits(SPLITS_MULTIPLIER);
		
		logger.info("Computing SpreadCorrectRecommend using " + splitCount + " splits");

		int splitPrime = splitStrategy.computeSplitPrime(splitCount, in.getNumPartitions());
		JavaPairRDD<Long, Iterable<Recommendation>> output = this.computeSplit(in, 0, splitCount, splitPrime,
				splitStrategy);

		for (int i = 1; i < splitCount; i++) {
			output.count();
			JavaPairRDD<Long, Iterable<Recommendation>> split = this.computeSplit(in, i, splitCount, splitPrime,
					splitStrategy);

			output = output.fullOuterJoin(split, new HashPartitioner(in.getNumPartitions())).mapToPair(e -> {
				List<Recommendation> joinedData = new ArrayList<>();
				Iterator<Recommendation> it;
				if (e._2._1.isPresent()) {
					it = e._2._1.get().iterator();
					while (it.hasNext()) {
						joinedData.add(it.next());
					}
				}
				if (e._2._2.isPresent()) {
					it = e._2._2.get().iterator();
					while (it.hasNext()) {
						joinedData.add(it.next());
					}
				}
				return new Tuple2<>(e._1, joinedData);
			});
		}

		return output;
	}
}

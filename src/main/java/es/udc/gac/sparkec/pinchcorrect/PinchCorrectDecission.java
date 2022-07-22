package es.udc.gac.sparkec.pinchcorrect;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.util.LongAccumulator;

import es.udc.gac.sparkec.node.EncodingUtils;
import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.sequence.IDNASequence;
import scala.Tuple2;

/**
 * PinchCorrectDecission is a SubPhase of the PinchCorrect Phase. This SubPhase will apply the recommendations
 * that have been previously emitted by the PinchCorrectRecommendation SubPhase.
 */
public class PinchCorrectDecission implements Serializable {
	

	private static final long serialVersionUID = 1L;

	/**
	 * The k being used at the current execution.
	 */
	private final int k;

	/**
	 * Number of reads that had at lest one correction applied.
	 */
	private final LongAccumulator fixReads;

	/**
	 * Number of chars that were corrected.
	 */
	private final LongAccumulator fixChar;

	/**
	 * Number of chars that were not corrected because they were too near to another
	 * correction.
	 */
	private final LongAccumulator skipChar;

	/**
	 * Number of nodes processed.
	 */
	private final LongAccumulator totales;

	/**
	 * Returns the total value of the FixReads accumulator.
	 * @return The total value of the FixReads accumulator
	 */
	public long getFixReads() {
		return fixReads.value();
	}

	/**
	 * Returns the total value of the FixChar accumulator.
	 * @return The total value of the FixChar accumulator
	 */
	public long getFixChar() {
		return fixChar.value();
	}

	/**
	 * Returns the total value of the SkipChar accumulator.
	 * @return The total value of the SkipChar accumulator
	 */
	public long getSkipChar() {
		return skipChar.value();
	}

	/**
	 * Returns the total value of the Totales accumulator.
	 * @return The total value of the Totales accumulator
	 */
	public long getTotales() {
		return totales.value();
	}

	/**
	 * Applies a list of recommendations to a single Node.
	 * @param element The original Node, with optionally a list of recommendations to be applied
	 * @param k The k being used by the current execution
	 * @param totals The totals accumulator to report back
	 * @param fixChar The fixChar accumulator to report back
	 * @param skipChar The skipChar accumulator to report back
	 * @param fixReads The fixReads accumulator to report back
	 * @return The Node, with the recommendations applied to it
	 */
	public static Tuple2<Long, Node> applyRecommendations(
			Tuple2<Long, Tuple2<Node, Optional<Iterable<PinchCorrectSingleRecommendationData>>>> element, int k,
			LongAccumulator totals, LongAccumulator fixChar, LongAccumulator skipChar, LongAccumulator fixReads) {

		totals.add(1L);

		if (!element._2._2.isPresent()) {
			return new Tuple2<>(element._1, element._2._1);
		}

		Iterable<PinchCorrectSingleRecommendationData> rec = element._2._2.get();
		Node node = element._2._1;

		IDNASequence seq = node.getSeq();

		// array: [0]=A, [1]=T, [2]=C, [3]=G, [4]=Sum
		int[][] array = new int[seq.length()][5];

		boolean[] skip = new boolean[seq.length()];

		for (int i = 0; i < seq.length(); i++) {
			skip[i] = false;

			for (int j = 0; j < 5; j++) {
				array[i][j] = 0;
			}
		}

		Iterator<PinchCorrectSingleRecommendationData> it = rec.iterator();

		while (it.hasNext()) {
			PinchCorrectSingleRecommendationData recommendation = it.next();
			int pos = recommendation.getPos();
			int base = EncodingUtils.char2idx((char) recommendation.getRecommendedBase());
			array[pos][base]++;

			if (base != EncodingUtils.char2idx('N')) {
				array[pos][4]++;
			}
		}

		// check if corrections are far away enough
		for (int i = 0; i < array.length; i++) {
			if (array[i][4] != 0) {
				for (int j = i + 1; j < array.length; j++) {
					if (array[j][4] != 0) {
						if (j - i <= k / 2) {
							skip[i] = true;
							skip[j] = true;
						}

						i = j - 1;

						break;
					}
				}
			}
		}

		// fix content

		IDNASequence newSeq = seq.copy();
		byte[] newQv = Arrays.copyOf(node.getQv(), seq.length());

		boolean corrected = false;

		for (int i = 0; i < array.length; i++) {

			if (array[i][4] > 0) { // If recommend sum is positive
				char fix_char = 'X';

				// The recommendation must be there
				for (int base = 0; base < 4; base++) {
					if (array[i][base] == array[i][4]) { // If global recommendations equals this base
															// recommendations
						fix_char = EncodingUtils.idx2char(base); // Then recommend base
						break;
					}
				}

				if (fix_char != 'X') { // If a recommendation was made

					if (!skip[i]) {
						newSeq.setValue(i, (byte) fix_char);
						newQv[i] = (byte) EncodingUtils.QV_FIX;

						corrected = true;

						fixChar.add(1);
					} else {

						skipChar.add(1);
					}
				}
			}
		}

		if (corrected) {
			fixReads.add(1);

		}

		node.setSeq(newSeq); // Seq update
		node.setQv(newQv);
		node.setCoverage(0);

		// node.setCoverage(0); // Must emmit as 0 coverage

		return new Tuple2<>(element._1, node);
	}

	/**
	 * Runs this SubPhase.
	 * @param recommendations The recommendations to apply
	 * @param nodes The nodes where the recommendations are being applied
	 * @return The nodes after applying the recommendations
	 */
	public JavaPairRDD<Long, Node> run(JavaPairRDD<Long, PinchCorrectSingleRecommendationData> recommendations,
			JavaPairRDD<Long, Node> nodes) {

		/**
		 * We then group all the received data by their node ID.
		 */
		JavaPairRDD<Long, Iterable<PinchCorrectSingleRecommendationData>> recommendationsGrouped;
		recommendationsGrouped = recommendations.groupByKey();

		JavaPairRDD<Long, Tuple2<Node, Optional<Iterable<PinchCorrectSingleRecommendationData>>>> recommendationsJoined;
		recommendationsJoined = nodes.leftOuterJoin(recommendationsGrouped, new HashPartitioner(nodes.getNumPartitions()));

		/**
		 * We receive the suggestion data with the node data all grouped for their node
		 * ID from the previous sub-phase and, now, we will check which bases were found
		 * far enough; and those who survive this comprobation will be applied to the
		 * original sequence string. We will emit our output in the next format: KEY:
		 * NodeID VALUE: Node with their fields modified (if there was any change at
		 * all).
		 */
		JavaPairRDD<Long, Node> result;
		result = recommendationsJoined
				.mapToPair(element -> applyRecommendations(element, k, totales, fixChar, skipChar, fixReads));

		return result;
	}

	/**
	 * Default constructor for PinchCorrectDecission.
	 * @param k The k currently being used by this execution
	 * @param jsc The JavaSparkContext
	 */
	public PinchCorrectDecission(int k, JavaSparkContext jsc) {
		this.k = k;
		this.fixReads = jsc.sc().longAccumulator();
		this.fixChar = jsc.sc().longAccumulator();
		this.skipChar = jsc.sc().longAccumulator();
		this.totales = jsc.sc().longAccumulator();
	}
}

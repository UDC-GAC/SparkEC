package es.udc.gac.sparkec.spreadcorrect;

import java.io.Serializable;
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
 * Subphase of the SpreadCorrect phase. this SubPhase takes the recommendations that have been made by 
 * SpreadCorrectRecommendation, and applies some of them as changes over the Sequences.
 */
public class SpreadCorrectDecission implements Serializable {

	private static final long serialVersionUID = 1L;
	
	/**
	 * Default quality value that will be used when a recommendation is applied.
	 */
	private static final short QV_FLATE_FIX = EncodingUtils.QV_FIX;

	/**
	 * Corrected accumulator.
	 */
	private final LongAccumulator correcteds;
	
	/**
	 * Total accumulator.
	 */
	private final LongAccumulator total;
	
	/**
	 * FixChar accumulator.
	 */
	private final LongAccumulator fixChar;
	
	/**
	 * Conflicts accumulator.
	 */
	private final LongAccumulator conflicts;

	/**
	 * Returns the total value of the corrected accumulator.
	 * @return The total value of the corrected accumulator
	 */
	public long getCorrecteds() {
		return correcteds.value();
	}

	/**
	 * Returns the total value of the total accumulator.
	 * @return The total value of the total accumulator
	 */
	public long getTotal() {
		return total.value();
	}

	/**
	 * Returns the FixChar value of the total accumulator.
	 * @return The total value of the total accumulator
	 */
	public long getFixChar() {
		return fixChar.value();
	}

	/**
	 * Returns the total value of the conflict accumulator
	 * @return The total value of the conflict accumulator
	 */
	public long getConflicts() {
		return conflicts.value();
	}

	/**
	 * Default constructor for SpreadCorrectDecission.
	 * @param jsc The JavaSparkContext
	 */
	public SpreadCorrectDecission(JavaSparkContext jsc) {
		correcteds = jsc.sc().longAccumulator();
		total = jsc.sc().longAccumulator();
		fixChar = jsc.sc().longAccumulator();
		conflicts = jsc.sc().longAccumulator();
	}

	/**
	 * Apply the suggested recommendations to each node
	 * 
	 * @param data       Tuple containing the ID of the node, the node itself, and
	 *                   optionally a list of recommendations to apply
	 * @param fixChar    Accumulator to report back the number of chars fixed
	 * @param correcteds Accumulator to report back the number of nodes corrected
	 * @param conflicts  Accumulator to report back the number of conflicts that did
	 *                   not allow to make a correction
	 * @param total      Accumulator to report back the number of nodes processed
	 * @return The node with the corrections applied
	 */
	public static Tuple2<Long, Node> applyRecommendations(
			Tuple2<Long, Tuple2<Optional<Iterable<Recommendation>>, Node>> data, LongAccumulator fixChar,
			LongAccumulator correcteds, LongAccumulator conflicts, LongAccumulator total) {
		Node node = data._2._2;
		if (data._2._1.isPresent()) {

			Iterator<Recommendation> it;

			// array: [0]=A, [1]=T, [2]=C, [3]=G, [4]=N, [5]=notN
			int[][] array = new int[node.getSeq().length()][6];
			for (int i = 0; i < node.getSeq().length(); i++) {
				for (int j = 0; j < 6; j++) {
					array[i][j] = 0;
				}
			}

			it = data._2._1.get().iterator();
			while (it.hasNext()) {
				Recommendation rec = it.next();
				int pos = rec.getPos();
				int base = EncodingUtils.char2idx((char)rec.getBase());

				array[pos][base]++;

				if (base != EncodingUtils.char2idx('N')) {
					array[pos][5]++;
				}
			}

			// fix content
			IDNASequence newSeq = node.getSeq().copy();
			byte[] newQv = EncodingUtils.qvSmooth(EncodingUtils.qvDeflate(node.getQv()));

			for (int i = 0; i < array.length; i++) {
				// A base cannot be corrected if it is in protection
				if (array[i][4] > 0) {

				} else if (array[i][5] > 0) {
					char fix_char = 'X';

					// The recommendations must be the same
					for (int base = 0; base < 4; base++) {
						if (array[i][base] == array[i][5]) {
							fix_char = EncodingUtils.idx2char(base);
							break;
						}
					}

					if (fix_char != 'X') {
						newSeq.setValue(i, (byte) fix_char);
						newQv[i] = QV_FLATE_FIX;
						fixChar.add(1);
					} else {
						conflicts.add(1);
					}
				}
			}

			node.setSeq(newSeq);
			node.setQv(EncodingUtils.qvInflate(newQv));
			correcteds.add(1);
		}
		total.add(1);
		return new Tuple2<>(data._1, node);
	}

	/**
	 * Runs this SubPhase.
	 * @param recommendations The recommendations data
	 * @param nodesDataIn The nodes data
	 * @return The modified nodes data
	 */
	public JavaPairRDD<Long, Node> run(JavaPairRDD<Long, Iterable<Recommendation>> recommendations,
			JavaPairRDD<Long, Node> nodesDataIn) {

		/**
		 * First, we join our recommendations data with our nodes data.
		 */
		JavaPairRDD<Long, Tuple2<Optional<Iterable<Recommendation>>, Node>> dataJoined;
		dataJoined = recommendations.rightOuterJoin(nodesDataIn, new HashPartitioner(nodesDataIn.getNumPartitions()));

		/**
		 * Finally, we apply the recommendations to the necessary nodes, and leave
		 * unmodified the ones without recommendations.
		 */
		JavaPairRDD<Long, Node> result;
		result = dataJoined.mapToPair(data -> applyRecommendations(data, fixChar, correcteds, conflicts, total));

		return result;
	}

}

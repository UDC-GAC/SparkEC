package es.udc.gac.sparkec.preprocess;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import es.udc.gac.sparkec.node.EncodingUtils;
import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.sequence.IDNASequence;
import scala.Tuple2;

/**
 * SubPhase of the PreProcess phase. This SubPhase will filter out all the invalid reads, and format the
 * valid ones.
 */
public class PreProcessCheck implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The k being used by this execution.
	 */
	private final int k;
	
	/**
	 * Whether the PinchCorrect check must be performed.
	 */
	private final boolean PINCHCORRECT;
	
	/**
	 * Whether the FilterReads check must be performed.
	 */
	private final boolean FILTERREADS;
	
	/**
	 * The ReadsSkipped accumulator.
	 */
	private final LongAccumulator reads_skipped;
	
	/**
	 * The ReadsPoly accumulator.
	 */
	private final LongAccumulator reads_poly;
	
	/**
	 * The ReadsGood accumulator.
	 */
	private final LongAccumulator reads_good;

	/**
	 * Default constructor for PreProcessCheck.
	 * @param k The k being used by this execution
	 * @param PINCHCORRECT Whether the PinchCorrect check must be performed
	 * @param FILTERREADS Whether the FilterReads check must be performed
	 * @param jsc The JavaSparkContext
	 */
	public PreProcessCheck(int k, boolean PINCHCORRECT, boolean FILTERREADS, JavaSparkContext jsc) {
		this.k = k;
		this.PINCHCORRECT = PINCHCORRECT;
		this.FILTERREADS = FILTERREADS;
		this.reads_skipped = jsc.sc().longAccumulator();
		this.reads_poly = jsc.sc().longAccumulator();
		this.reads_good = jsc.sc().longAccumulator();
	}

	/**
	 * Returns the total value of the ReadsSkipped accumulator.
	 * @return The total value of the ReadsSkipped accumulator
	 */
	public long getReads_skipped() {
		return reads_skipped.sum();
	}

	/**
	 * Returns the total value of the ReadsPoly accumulator.
	 * @return The total value of the ReadsPoly accumulator
	 */
	public long getReads_poly() {
		return reads_poly.sum();
	}

	/**
	 * Returns the total value of the ReadsGood accumulator.
	 * @return The total value of the ReadsGood accumulator
	 */
	public long getReads_good() {
		return reads_good.sum();
	}

	/**
	 * Checks if a node is valid. The checks performed are:
	 * <ul>
	 * <li>Whether the bases sequence has the same length as the qualities sequence
	 * <li>Whether the length of the sequence is less than the k selected
	 * <li>If filterReads is enabled, whether the sequence has not any invalid base.
	 * If it's not, it just only checks that there are not too much 'N' bases.
	 * <li>Whether the sequence has not too much 'A' bases
	 * </ul>
	 * 
	 * @param node              The node to check
	 * @param readsSkipped      Accumulator to report back the number of reads that
	 *                          had an invalid format
	 * @param readsPoly         Accumulator to report back the number of reads that
	 *                          were filtered because they had too much bases of 'N'
	 *                          or 'A'
	 * @param readsGood         Accumulator to report back the number of valid reads
	 * @param k                 The k that will be used by CloudEC
	 * @param pinchCorrectCheck Whether PinchCorrect will be enabled and (k+1)-mers
	 *                          will be generated
	 * @param filterReadsCheck  If an strict base check should be performed
	 * @return Whether the base was valid
	 */
	public static boolean isValid(Tuple2<String, Node> node, LongAccumulator readsSkipped, LongAccumulator readsPoly,
			LongAccumulator readsGood, int k, boolean pinchCorrectCheck, boolean filterReadsCheck) {

		IDNASequence seq = node._2.getSeq();
		String seqS = new String(seq.toByteArray());
		byte[] qv = node._2.getQv();

		if (seq.length() != qv.length) {
			readsSkipped.add(1);
			return false;
		}

		if (seq.length() < k + (pinchCorrectCheck ? 1 : 0)) {
			readsSkipped.add(1);
			return false;
		}

		if (filterReadsCheck) {
			if (seqS.matches(".*[^ACGT].*")) {
				readsSkipped.add(1);
				return false;
			}
		} else {
			if (seq.isPoly((byte)'N')) {
				readsPoly.add(1);
				return false;
			}

		}

		if (seq.isPoly((byte)'A')) {
			readsPoly.add(1);
			return false;
		}

		readsGood.add(1);
		EncodingUtils.qvInputConvert(node._2.getQv(), true);
		return true;
	}

	/**
	 * Encodes the quality value into the internal format.
	 * @param node The node, with the qualities unencoded
	 * @return The node, with the qualities encoded
	 */
	public static Tuple2<String, Node> qvInputConvert(Tuple2<String, Node> node) {
		EncodingUtils.qvInputConvert(node._2.getQv(), true);
		return node;
	}

	/**
	 * Runs this SubPhase.
	 * @param data The read Nodes data
	 * @return The filtered and formatted Nodes data
	 */
	public Tuple2<JavaPairRDD<Long, String>, JavaPairRDD<Long, Node>> run(JavaPairRDD<String, Node> data) {

		JavaPairRDD<String, Node> filteredData;
		filteredData = data
				.filter(node -> isValid(node, reads_skipped, reads_poly, reads_good, k, PINCHCORRECT, FILTERREADS));

		JavaPairRDD<String, Node> formattedData;
		formattedData = filteredData.mapToPair(PreProcessCheck::qvInputConvert);
		
		JavaPairRDD<Tuple2<String, Node>, Long> zipped;
		zipped = formattedData.zipWithUniqueId();
		
		JavaPairRDD<Long, String> mappings;
		mappings = zipped.mapToPair(e -> new Tuple2<>(e._2, e._1._1));
		
		JavaPairRDD<Long, Node> resultingNodes;
		resultingNodes = zipped.mapToPair(e -> new Tuple2<>(e._2, e._1._2));

		return new Tuple2<JavaPairRDD<Long, String>, JavaPairRDD<Long, Node>>(mappings, resultingNodes);
	}

}

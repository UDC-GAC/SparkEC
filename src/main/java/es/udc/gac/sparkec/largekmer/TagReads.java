package es.udc.gac.sparkec.largekmer;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import es.udc.gac.sparkec.node.EncodingUtils;
import es.udc.gac.sparkec.node.Node;
import scala.Tuple2;

/**
 * TagReads is a SubPhase of the LargeKmerFilter Phase. It takes care of tagging the Nodes, using the
 * Node ignore data generated at CountKmers.
 */
public class TagReads implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The k currently being used at this execution.
	 */
	private final int k;

	/**
	 * Default constructor for TagReads
	 * @param jsc The JavaSparkContext
	 * @param k The k currently being used at this execution.
	 */
	public TagReads(JavaSparkContext jsc, int k) {
		this.k = k;
	}

	/**
	 * Emit nodes tagged from the node kmers data
	 * 
	 * @param nodeKmersData The node kmers data
	 * @param k             The k used by CloudEC
	 * @return The node tagged
	 */
	public static Tuple2<Long, Node> tagNode(Tuple2<Long, KmerCount> nodeKmersData, int k) {

		if (nodeKmersData._2.node.getSeq().length() - k > 0) {
			int len = nodeKmersData._2.node.getSeq().length() - k + 1;
			
			byte[] listF = new byte[(int) (Math.ceil(len / 4.0) * 4)];
			byte[] listP = new byte[(int) (Math.ceil((len - 1) / 4.0) * 4)];

			// set up the ignore position
			for (Integer p : nodeKmersData._2.count_F) {
				listF[p] = 1;
			}
			for (Integer p : nodeKmersData._2.count_P) {
				listP[p] = 1;
			}

			// append the ignore list to node
			if (!nodeKmersData._2.count_F.isEmpty()) {
				nodeKmersData._2.node.setIgnF(new String(EncodingUtils.str2hex(listF)));
			}
			if (!nodeKmersData._2.count_P.isEmpty()) {
				nodeKmersData._2.node.setIgnP(new String(EncodingUtils.str2hex(listP)));
			}
		}
		return new Tuple2<>(nodeKmersData._1, nodeKmersData._2.node);
	}

	/**
	 * Runs this SubPhase.
	 * @param kmers The input dataset
	 * @return The tagged nodes
	 */
	public JavaPairRDD<Long, Node> run(JavaPairRDD<Long, KmerCount> kmers) {

		JavaPairRDD<Long, Node> modifiedNodes;
		modifiedNodes = kmers.mapToPair(data -> tagNode(data, k));

		return modifiedNodes;
	}
}

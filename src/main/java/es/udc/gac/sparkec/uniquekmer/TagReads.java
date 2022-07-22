package es.udc.gac.sparkec.uniquekmer;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.util.LongAccumulator;

import es.udc.gac.sparkec.node.Node;
import scala.Tuple2;

/**
 * SubPhase of the UniqueKmerFilter that takes care of tagging the sequences with the filter data.
 */
public class TagReads implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Accumulator for the unique reads found.
	 */
	private final LongAccumulator uniqueReads;

	/**
	 * Default constructor for TagReads
	 * @param jsc The JavaSparkContext
	 */
	public TagReads(JavaSparkContext jsc) {
		uniqueReads = jsc.sc().longAccumulator();
	}

	/**
	 * Returns the unique reads found.
	 * @return The unique reads found
	 */
	public long getUniqueReads() {
		return uniqueReads.value();
	}

	/**
	 * Runs this SubPhase.
	 * @param nodesCounted The node count data generated by CountKmers
	 * @param in The original node data
	 * @return The updated node data
	 */
	public JavaPairRDD<Long, Node> run(JavaPairRDD<Long, Integer> nodesCounted, JavaPairRDD<Long, Node> in) {

		/**
		 * We join our original node data with our nodes counted.
		 */
		JavaPairRDD<Long, Tuple2<Node, Optional<Integer>>> nodesJoined;
		nodesJoined = in.leftOuterJoin(nodesCounted);

		/**
		 * If there was a node previously counted, we then set the adequate field
		 * through the method setOrRemoveUnique, and we emmit the node as text.
		 */
		JavaPairRDD<Long, Node> result;
		result = nodesJoined.mapToPair(data -> {
			Optional<Integer> counted;
			counted = data._2._2;

			Node node = data._2._1;
			boolean unique = counted.isPresent();
			if (unique) {
				uniqueReads.add(1);
			}
			node.setUnique(unique);

			return new Tuple2<>(data._1, node);
		});

		return result;
	}
}

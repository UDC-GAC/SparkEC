package es.udc.gac.sparkec.postprocess;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Objects;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import es.udc.gac.sparkec.node.EncodingUtils;
import es.udc.gac.sparkec.node.Node;
import scala.Tuple2;

/**
 * SubPhase of the PostProcess Phase. This SubPhase will generate global statistics
 * about the corrected nodes.
 */
public class Merge implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The ReadsIGN accumulator.
	 */
	private final LongAccumulator readsIgn;
	
	/**
	 * The ReadsEC accumulator.
	 */
	private final LongAccumulator readsEC;
	
	/**
	 * The ReadsFail accumulator.
	 */
	private final LongAccumulator readsFail;

	/**
	 * Default constructor for the Merge SubPhase.
	 * @param jsc The JavaSparkContext
	 */
	public Merge(JavaSparkContext jsc) {
		readsIgn = jsc.sc().longAccumulator();
		readsEC = jsc.sc().longAccumulator();
		readsFail = jsc.sc().longAccumulator();
	}

	/**
	 * Returns the total value of the ReadsIGN accumulator.
	 * @return The total value of the ReadsIGN accumulator
	 */
	public long getReadsIgn() {
		return readsIgn.value();
	}

	/**
	 * Returns the total value of the ReadsEC accumulator.
	 * @return The total value of the ReadsEC accumulator
	 */
	public long getReadsEC() {
		return readsEC.value();
	}

	/**
	 * Returns the total value of the ReadsFail accumulator.
	 * @return The total value of the ReadsFail accumulator
	 */
	public long getReadsFail() {
		return readsFail.value();
	}

	/**
	 * Runs this SubPhase.
	 * @param currentNodes The nodes after all the correction and filter Phases
	 * @param startingNodes The input nodes
	 * @return The nodes after the stats have been populated
	 */
	public JavaPairRDD<Long, Node> run(JavaPairRDD<Long, Node> currentNodes,
			JavaPairRDD<Long, Node> startingNodes) {

		JavaPairRDD<Long, Node> nodes;
		nodes = currentNodes.union(startingNodes);

		JavaPairRDD<Long, Iterable<Node>> nodesGrouped;
		nodesGrouped = nodes.groupByKey();

		JavaPairRDD<Long, Node> output;
		output = nodesGrouped.mapToPair(e -> {
			Iterator<Node> it = e._2.iterator();
			int nodeCount = 0;
			Node out = null;
			while (it.hasNext()) {
				nodeCount++;
				out = it.next();
			}

			if (nodeCount == 1) {
				readsIgn.add(1L);
			} else if (nodeCount == 2) {
				readsEC.add(1L);
			} else {
				readsFail.add(1L);
				return null;
			}

			EncodingUtils.qvInputConvert(out.getQv(), true);
			return new Tuple2<>(e._1, out);
		}).filter(Objects::nonNull);

		return output;

	}
}

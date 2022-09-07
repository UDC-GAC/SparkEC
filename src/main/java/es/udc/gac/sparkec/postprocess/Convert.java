package es.udc.gac.sparkec.postprocess;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import es.udc.gac.sparkec.node.EncodingUtils;
import es.udc.gac.sparkec.node.Node;
import scala.Tuple2;

/**
 * SubPhase of the PostProcess Phase.
 */
public class Convert implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The UniqueReads accumulator.
	 */
	private final LongAccumulator uniqueReads;
	
	/**
	 * The OutputReads accumulator.
	 */
	private final LongAccumulator outputReads;

	/**
	 * Default constructor for the Convert SubPhase.
	 * @param jsc The JavaSparkContext
	 */
	public Convert(JavaSparkContext jsc) {
		uniqueReads = jsc.sc().longAccumulator();
		outputReads = jsc.sc().longAccumulator();
	}

	/**
	 * Returns the total value of the UniqueReads accumulator.
	 * @return The total value of the UniqueReads accumulator
	 */
	public long getUniqueReads() {
		return uniqueReads.value();
	}

	/**
	 * Returns the total value of the OutputReads accumulator.
	 * @return The total value of the OutputReads accumulator
	 */
	public long getOutputReads() {
		return outputReads.value();
	}

	/**
	 * Runs this SubPhase.
	 * @param in The nodes to output
	 * @param mappingJavaPairRDD The ID mappings for the nodes
	 * @return The nodes string RDD
	 */
	public JavaRDD<String> run(JavaPairRDD<Long, Node> in, JavaPairRDD<Long, String> mappingJavaPairRDD) {
		
		/**
		 * First, we filter out the unique reads.
		 */
		JavaPairRDD<Long, Node> nodesFiltered;
		nodesFiltered = in.filter(data -> {

			if (Boolean.TRUE.equals(data._2.isUnique())) {
				uniqueReads.add(1);
				return false;
			} else {
				outputReads.add(1);
				return true;
			}
		});
		
		/**
		 * Then, we join them with their original IDs
		 */
		JavaPairRDD<String, Node> nodesMapped;
		nodesMapped = nodesFiltered.join(mappingJavaPairRDD).mapToPair(e -> new Tuple2<>(e._2._2, e._2._1));
		
		/**
		 * Finally, we output them in the required format.
		 */
		JavaRDD<String> result;
		result = nodesMapped.map(data -> "@" + data._1 + "\n" + data._2.getSeq().toString() + "\n+" + data._1 + "\n"
				+ EncodingUtils.qvOutputConvert(new String(data._2.getQv())));

		return result;
	}
}

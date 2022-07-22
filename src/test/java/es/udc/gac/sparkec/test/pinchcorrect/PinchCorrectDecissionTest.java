/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.udc.gac.sparkec.test.pinchcorrect;

import static com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.sparkec.node.EncodingUtils;
import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.pinchcorrect.PinchCorrectDecission;
import es.udc.gac.sparkec.pinchcorrect.PinchCorrectSingleRecommendationData;
import es.udc.gac.sparkec.test.utils.DataSetNotFoundException;
import es.udc.gac.sparkec.test.utils.PhaseTest;
import scala.Tuple2;

public class PinchCorrectDecissionTest extends PhaseTest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private PinchCorrectDecission task;

	public PinchCorrectDecissionTest() {
		super("pinchcorrect_Decission", new String[] { "in_kmers", "in_nodes", "out" });
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.task = new PinchCorrectDecission(this.config.getK(), this.config.getJavaSparkContext());
	}

	/**
	 * Test of run method, of class PinchCorrectDecission.
	 * 
	 * @throws es.udc.gac.sparkec.test.utils.DataSetNotFoundException
	 */
	@Test
	public void testRun() throws DataSetNotFoundException {

		JavaRDD<String> nodesIn = this.getDataset("in_nodes");
		JavaRDD<String> kmersIn = this.getDataset("in_kmers");
		JavaPairRDD<String, Node> expectedOut = this.getDataset("out").mapToPair(Node::fromCloudECString);

		/**
		 * We start by splitting the nodes input into NodeID and Node data.
		 */
		JavaPairRDD<String, Node> processedNodesIn;
		processedNodesIn = nodesIn.mapToPair(Node::fromCloudECString);

		/**
		 * We read the CloudEC-like encoded corrections data, and format it to the
		 * expected format. INPUT: NodeID + "\t" + "A" + "\t" + StringEncodedCorrections
		 * 
		 * OUTPUT: NodeID + "\t" + "A" + "\t" + pos + "\t" + base
		 */
		JavaPairRDD<String, PinchCorrectSingleRecommendationData> kmersInProcessed = kmersIn.flatMapToPair(element -> {

			List<Tuple2<String, PinchCorrectSingleRecommendationData>> result = new LinkedList<>();
			String[] parts = element.split("\t");
			if ((parts.length != 3) || (!parts[1].equals("A"))) {
				throw new RuntimeException();
			}
			String corrStr = EncodingUtils.corrDecode(parts[2]);
			int i = 0;
			for (char c : corrStr.toCharArray()) {
				if (c == 'X') {
					i++;
				} else {
					char newBase = c;
					int newPos = i;
					result.add(
							new Tuple2<>(parts[0], new PinchCorrectSingleRecommendationData((byte) newBase, newPos)));
				}
			}

			return result.iterator();
		});

		Tuple2<JavaPairRDD<Long, Node>, JavaPairRDD<Long, String>> processedNodesInMapped;
		processedNodesInMapped = this.splitWithMapping(processedNodesIn);

		JavaPairRDD<Long, PinchCorrectSingleRecommendationData> kmersInMapped;
		kmersInMapped = this.splitUsingMapping(kmersInProcessed, processedNodesInMapped._2);

		/**
		 * Finished the data preprocess, we start the execution of the phase.
		 */
		JavaPairRDD<Long, Node> out;
		out = this.task.run(kmersInMapped, processedNodesInMapped._1);

		JavaPairRDD<String, Node> outMapped;
		outMapped = this.joinWithMapping(out, processedNodesInMapped._2);

		/**
		 * After this execution, we join the corrected nodes with the unmodified ones.
		 */
		JavaPairRDD<String, Tuple2<Node, Optional<Node>>> joinedOut;
		joinedOut = processedNodesIn.leftOuterJoin(outMapped);

		/**
		 * Finally, we regenerate the original format, using when possible the modified
		 * nodes.
		 */
		JavaPairRDD<String, Node> processedOut;
		processedOut = joinedOut.mapToPair(element -> {
			Node node = null;
			if (element._2._2.isPresent()) {
				node = element._2._2.get();
			} else {
				node = element._2._1;
			}
			node.setCoverage(0.0f);
			return new Tuple2<>(element._1, node);
		});

		assertRDDEquals(expectedOut.map(e -> e), processedOut.map(e -> e));
	}

}

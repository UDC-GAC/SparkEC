/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.udc.gac.sparkec.test.pinchcorrect;

import static com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.sparkec.node.EncodingUtils;
import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.pinchcorrect.PinchCorrectRecommendation;
import es.udc.gac.sparkec.pinchcorrect.PinchCorrectSingleRecommendationData;
import es.udc.gac.sparkec.test.utils.DataSetNotFoundException;
import es.udc.gac.sparkec.test.utils.PhaseTest;
import scala.Tuple2;

/**
 *
 * @author marco
 */
public class PinchCorrectRecommendationTest extends PhaseTest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private PinchCorrectRecommendation task;

	public PinchCorrectRecommendationTest() {
		super("pinchcorrect_Recommend", new String[] { "in", "out" });

	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.task = new PinchCorrectRecommendation(this.config.getK(), this.config.getJavaSparkContext());
	}

	/**
	 * Test of run method, of class PinchCorrectRecommendation.
	 * 
	 * @throws es.udc.gac.sparkec.test.utils.DataSetNotFoundException When a needed
	 *                                                                 testing
	 *                                                                 DataSet was
	 *                                                                 not found.
	 */
	@Test
	public void testRun() throws DataSetNotFoundException {
		JavaRDD<String> in = this.getDataset("in");
		JavaRDD<String> expectedOutput = this.getDataset("out");

		JavaPairRDD<String, Node> formattedInput;
		formattedInput = in.mapToPair(Node::fromCloudECString);

		Tuple2<JavaPairRDD<Long, Node>, JavaPairRDD<Long, String>> inOriMapped;
		inOriMapped = this.splitWithMapping(formattedInput);

		JavaPairRDD<Long, PinchCorrectSingleRecommendationData> out = task.run(inOriMapped._1, this.getSplitStrategy());

		JavaPairRDD<String, PinchCorrectSingleRecommendationData> outMapped;
		outMapped = this.joinWithMapping(out, inOriMapped._2);

		/**
		 * Then, we split the output into the following format: KEY: NodeID VALUE: "A" +
		 * "\t" + pos + "\t" + base
		 */
		JavaPairRDD<String, String> splitOut;
		splitOut = outMapped.mapToPair(element -> new Tuple2<>(element._1,
				"A\t" + element._2.getPos() + "\t" + ((char) element._2.getRecommendedBase())));

		/**
		 * Then, we group them by their key.
		 */
		JavaPairRDD<String, Iterable<String>> groupedSplitOut;
		groupedSplitOut = splitOut.groupByKey();

		/**
		 * Then, we join this data with the original node data, in order to encode the
		 * output in the same format that the original CloudEC did.
		 */
		JavaPairRDD<String, Tuple2<Iterable<String>, Node>> joinedOutData;
		joinedOutData = groupedSplitOut.join(formattedInput);

		/**
		 * Finally, we output the data in the same format that the one used in CloudEC.
		 */
		JavaRDD<String> formattedOut;
		formattedOut = joinedOutData.map(e -> {
			Node node = e._2._2;

			char corrData[] = new char[node.getSeq().length()];
			for (int i = 0; i < corrData.length; i++) {
				corrData[i] = 'X';
			}
			for (String corr : e._2._1) {
				String[] parts = corr.split("\t");
				if ((parts.length != 3) || !parts[0].equals("A")) {
					throw new RuntimeException();
				}
				corrData[Integer.parseInt(parts[1])] = parts[2].charAt(0);
			}
			StringBuilder sb = new StringBuilder();
			for (char c : corrData) {
				if (c != 'X') {
					sb.append(c);
				}
				sb.append('X');
			}
			sb.deleteCharAt(sb.length() - 1);

			String encoded = EncodingUtils.corrEncode(sb.toString());
			return e._1 + "\tA\t" + encoded;
		});

		assertRDDEquals(formattedOut, expectedOutput);
	}

}

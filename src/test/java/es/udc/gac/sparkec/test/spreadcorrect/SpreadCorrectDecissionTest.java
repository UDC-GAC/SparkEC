package es.udc.gac.sparkec.test.spreadcorrect;

import static com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.spreadcorrect.Recommendation;
import es.udc.gac.sparkec.spreadcorrect.SpreadCorrectDecission;
import es.udc.gac.sparkec.test.utils.DataSetNotFoundException;
import es.udc.gac.sparkec.test.utils.PhaseTest;
import scala.Tuple2;

public class SpreadCorrectDecissionTest extends PhaseTest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private SpreadCorrectDecission task;

	public SpreadCorrectDecissionTest() {
		super("spreadcorrect_Decission", new String[] { "nodeIn", "recommendIn", "out" });
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.task = new SpreadCorrectDecission(this.config.getJavaSparkContext());
	}

	/**
	 * Test of run method, of class SpreadCorrectDecission.
	 * 
	 * @throws es.udc.gac.sparkec.test.utils.DataSetNotFoundException
	 */
	@Test
	public void testRun() throws DataSetNotFoundException {
		JavaPairRDD<String, Node> nodeIn = this.getDataset("nodeIn").mapToPair(Node::fromCloudECString);
		JavaRDD<String> recommendIn = this.getDataset("recommendIn");
		JavaPairRDD<String, Node> expectedOut = this.getDataset("out").mapToPair(Node::fromCloudECString);

		JavaPairRDD<String, String> recommendInSplit;
		recommendInSplit = recommendIn.mapToPair(e -> {
			String[] parts = e.split("\t", 2);

			return new Tuple2<>(parts[0], parts[1]);
		});

		JavaPairRDD<String, Iterable<String>> recommendInGrouped;
		recommendInGrouped = recommendInSplit.groupByKey();

		JavaPairRDD<String, Iterable<Recommendation>> processedRecommendIn;
		processedRecommendIn = recommendInGrouped.mapToPair((Tuple2<String, Iterable<String>> e) -> {
			List<Recommendation> outList = new LinkedList<>();
			Iterator<String> it = e._2.iterator();
			while (it.hasNext()) {
				outList.addAll(SpreadCorrectTestUtils.decodeRecommend(it.next()));
			}

			return new Tuple2<>(e._1, outList);
		});

		Tuple2<JavaPairRDD<Long, Node>, JavaPairRDD<Long, String>> processedNodesIn;
		processedNodesIn = this.splitWithMapping(nodeIn);
		
		JavaPairRDD<Long, Iterable<Recommendation>> processedRecommendInTagged;
		processedRecommendInTagged = this.splitUsingMapping(processedRecommendIn, processedNodesIn._2);
		
		JavaPairRDD<Long, Node> out;
		out = this.task.run(processedRecommendInTagged, processedNodesIn._1);
		
		JavaPairRDD<String, Node> outMapped;
		outMapped = this.joinWithMapping(out, processedNodesIn._2);

		assertRDDEquals(outMapped.map(e -> e), expectedOut.map(e -> e));

	}

}
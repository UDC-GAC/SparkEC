package es.udc.gac.sparkec.test.uniquekmer;

import static com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals;

import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.test.utils.DataSetNotFoundException;
import es.udc.gac.sparkec.test.utils.PhaseTest;
import es.udc.gac.sparkec.uniquekmer.TagReads;
import scala.Tuple2;

public class TagReadsTest extends PhaseTest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private TagReads task;

	public TagReadsTest() {
		super("uniquekmer_Tagreads", new String[] { "nodesIn", "countedIn", "out" });
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.task = new TagReads(this.config.getJavaSparkContext());
	}

	/**
	 * Test of run method, of class TagReads.
	 * 
	 * @throws es.udc.gac.sparkec.test.utils.DataSetNotFoundException
	 */
	@Test
	public void testRun() throws DataSetNotFoundException {
		JavaPairRDD<String, Node> nodesIn = this.getDataset("nodesIn").mapToPair(Node::fromCloudECString);
		JavaRDD<String> countedIn = this.getDataset("countedIn");
		JavaPairRDD<String, Node> expectedOut = this.getDataset("out").mapToPair(Node::fromCloudECString);

		JavaPairRDD<String, String> countedInSplit;
		countedInSplit = countedIn.mapToPair(e -> {
			String[] parts = e.split("\t", 2);
			return new Tuple2<>(parts[0], parts[1]);
		});

		JavaPairRDD<String, Iterable<String>> countedInGrouped;
		countedInGrouped = countedInSplit.groupByKey();

		JavaPairRDD<String, Integer> countedInProcessed;
		countedInProcessed = countedInGrouped.mapToPair(e -> {
			int i = 0;
			Iterator<String> it = e._2.iterator();
			while (it.hasNext()) {
				it.next();
				i++;
			}

			return new Tuple2<>(e._1, i);
		});

		
		
		
		Tuple2<JavaPairRDD<Long, Node>, JavaPairRDD<Long, String>> processedNodesIn;
		processedNodesIn = this.splitWithMapping(nodesIn);
		
		JavaPairRDD<Long, Integer> countedInProcessedTagged;
		countedInProcessedTagged = this.splitUsingMapping(countedInProcessed, processedNodesIn._2);
		
		JavaPairRDD<Long, Node> out;
		out = this.task.run(countedInProcessedTagged, processedNodesIn._1);
		
		JavaPairRDD<String, Node> outMapped;
		outMapped = this.joinWithMapping(out, processedNodesIn._2);

		assertRDDEquals(outMapped.map(e -> e), expectedOut.map(e -> e));
	}

}

package es.udc.gac.sparkec.test.postprocess;

import static com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.postprocess.Merge;
import es.udc.gac.sparkec.test.utils.DataSetNotFoundException;
import es.udc.gac.sparkec.test.utils.PhaseTest;
import scala.Tuple2;

/**
 *
 * @author marco
 */
public class MergeTest extends PhaseTest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Merge task;

	public MergeTest() {
		super("postprocess_Merge", new String[] { "in_processed", "in_ori", "out" });
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.task = new Merge(this.config.getJavaSparkContext());
	}

	/**
	 * Test of run method, of class Merge.
	 * 
	 * @throws es.udc.gac.sparkec.test.utils.DataSetNotFoundException
	 */
	@Test
	public void testRun() throws DataSetNotFoundException {
		JavaPairRDD<String, Node> inOri = this.getDataset("in_ori").mapToPair(Node::fromSFQString);
		JavaPairRDD<String, Node> inProcessed = this.getDataset("in_processed").mapToPair(Node::fromCloudECString);
		JavaPairRDD<String, Node> expectedOut = this.getDataset("out").mapToPair(Node::fromCloudECString);

		expectedOut = expectedOut.mapToPair(e -> {
			e._2.setCoverage(0.0f);
			return e;
		});

		Tuple2<JavaPairRDD<Long, Node>, JavaPairRDD<Long, String>> inOriMapped;
		inOriMapped = this.splitWithMapping(inOri);
		
		JavaPairRDD<Long, Node> inProcessedMapped;
		inProcessedMapped = this.splitUsingMapping(inProcessed, inOriMapped._2);
		
		JavaPairRDD<Long, Node> out = this.task.run(inOriMapped._1, inProcessedMapped);

		JavaPairRDD<String, Node> outProcessed;
		outProcessed = this.joinWithMapping(out, inOriMapped._2);
		
		
		assertRDDEquals(outProcessed.map(e -> e), expectedOut.map(e -> e));

	}

}

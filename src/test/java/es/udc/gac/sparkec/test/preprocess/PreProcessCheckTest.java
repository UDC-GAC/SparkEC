package es.udc.gac.sparkec.test.preprocess;

import static com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.preprocess.PreProcessCheck;
import es.udc.gac.sparkec.test.utils.DataSetNotFoundException;
import es.udc.gac.sparkec.test.utils.PhaseTest;
import scala.Tuple2;

/**
 *
 * @author marco
 */
public class PreProcessCheckTest extends PhaseTest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private PreProcessCheck task;

	public PreProcessCheckTest() {
		super("preprocess_Check", new String[] { "in", "out" });
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.task = new PreProcessCheck(this.config.getK(), true, true, this.config.getJavaSparkContext());
	}

	/**
	 * Test of run method, of class PreProcessTask.
	 * 
	 * @throws es.udc.gac.sparkec.test.utils.DataSetNotFoundException
	 */
	@Test
	public void testRun() throws DataSetNotFoundException {
		JavaPairRDD<String, Node> in = this.getDataset("in").mapToPair(Node::fromSFQString);
		JavaPairRDD<String, Node> expectedOut = this.getDataset("out").mapToPair(Node::fromCloudECString);

		expectedOut = expectedOut.mapToPair(e -> {
			e._2.setCoverage(0.0f);
			return e;
		});
		

		
		Tuple2<JavaPairRDD<Long, String>, JavaPairRDD<Long, Node>> out = this.task.run(in);

		assertRDDEquals(this.joinWithMapping(out._2, out._1).map(e -> e), expectedOut.map(e -> e));
	}

}

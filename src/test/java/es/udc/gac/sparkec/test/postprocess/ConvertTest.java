
package es.udc.gac.sparkec.test.postprocess;

import static com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.postprocess.Convert;
import es.udc.gac.sparkec.test.utils.DataSetNotFoundException;
import es.udc.gac.sparkec.test.utils.PhaseTest;
import scala.Tuple2;

public class ConvertTest extends PhaseTest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Convert task;

	public ConvertTest() {
		super("postprocess_Convert", new String[] { "in", "out" });
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.task = new Convert(this.config.getJavaSparkContext());
	}

	/**
	 * Test of run method, of class Convert.
	 * 
	 * @throws es.udc.gac.sparkec.test.utils.DataSetNotFoundException
	 */
	@Test
	public void testRun() throws DataSetNotFoundException {

		JavaRDD<String> expectedOut = this.getDataset("out");
		JavaRDD<String> in = this.getDataset("in");

		JavaPairRDD<String, Node> inProcessed;
		inProcessed = in.mapToPair(Node::fromCloudECString);
		
		
		Tuple2<JavaPairRDD<Long, Node>, JavaPairRDD<Long, String>> inMapped;
		inMapped = this.splitWithMapping(inProcessed);

		JavaRDD<String> out = this.task.run(inMapped._1, inMapped._2);

		JavaRDD<String> outProcessed;
		outProcessed = out.flatMap(e -> {
			return Arrays.asList(e.split("\n")).iterator();
		});

		
		List<String> outTmp = new ArrayList<>(outProcessed.collect());
		List<String> expTmp = new ArrayList<>(expectedOut.collect());
		
		outTmp.sort(String::compareTo);
		expTmp.sort(String::compareTo);
		
		assertRDDEquals(outProcessed, expectedOut);

	}

}

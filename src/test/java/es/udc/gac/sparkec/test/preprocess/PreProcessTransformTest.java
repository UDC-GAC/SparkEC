package es.udc.gac.sparkec.test.preprocess;

import static com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals;

import java.lang.reflect.Field;

import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.preprocess.PreProcessTransform;
import es.udc.gac.sparkec.test.utils.DataSetNotFoundException;
import es.udc.gac.sparkec.test.utils.PhaseTest;

/**
 *
 * @author marco
 */
public class PreProcessTransformTest extends PhaseTest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private PreProcessTransform task;

	public PreProcessTransformTest() {
		super("preprocess_Transform", new String[] { "internal_in", "internal_out", "FastQ_in", "FastQ_out" });
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.task = new PreProcessTransform(this.config.getInputType(), this.config.getJavaSparkContext(),
				this.config.getJavaSparkContext().sc().longAccumulator(),
				this.config.getJavaSparkContext().sc().longAccumulator());
	}

	/**
	 * Test of internal input type
	 * 
	 * @throws es.udc.gac.sparkec.test.utils.DataSetNotFoundException
	 * @throws SecurityException
	 * @throws NoSuchFieldException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void testInternal() throws DataSetNotFoundException, IllegalArgumentException, IllegalAccessException,
			NoSuchFieldException, SecurityException {

		testCase("internal");
	}

	/**
	 * Test of FastQ input type
	 * 
	 * @throws es.udc.gac.sparkec.test.utils.DataSetNotFoundException
	 * @throws SecurityException
	 * @throws NoSuchFieldException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void testFastQ() throws DataSetNotFoundException, IllegalArgumentException, IllegalAccessException,
			NoSuchFieldException, SecurityException {

		testCase("FastQ");
	}

	public void testCase(String type) throws DataSetNotFoundException, IllegalArgumentException, IllegalAccessException,
			NoSuchFieldException, SecurityException {

		JavaPairRDD<String, Node> expectedOut = this.getDataset(type + "_out").mapToPair(Node::fromSFQString);

		Field f = this.task.getClass().getDeclaredField("inputType");
		f.setAccessible(true);
		f.set(this.task, type);
		f.setAccessible(false);

		String inPath = this.getDatasetBasePath() + type + "_in.txt";
		JavaPairRDD<String, Node> out = this.task.run(inPath);

		assertRDDEquals(out.map(e -> e), expectedOut.map(e -> e));
	}
}

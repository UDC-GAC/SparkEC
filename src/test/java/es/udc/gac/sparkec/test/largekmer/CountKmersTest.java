/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.udc.gac.sparkec.test.largekmer;

import static com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.sparkec.largekmer.CountKmers;
import es.udc.gac.sparkec.largekmer.KmerCount;
import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.test.utils.DataSetNotFoundException;
import es.udc.gac.sparkec.test.utils.PhaseTest;
import scala.Tuple2;

/**
 *
 * @author marco
 */
public class CountKmersTest extends PhaseTest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// TASK REF
	private CountKmers task;

	public CountKmersTest() {
		super("largekmer_CountKmers", new String[] { "in", "out" });

	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.task = new CountKmers(this.config.getK(), this.config.getFilter_P(), this.config.getFilter_S(),
				this.config.getJavaSparkContext(), this.config.getStackMax(),
				this.config.getStackMin());
	}

	/**
	 * Test of run method, of class CountKmers.
	 */
	@Test
	public void testRun() throws DataSetNotFoundException {

		/**
		 * First, we format the input into the expected format.
		 */
		JavaPairRDD<String, Node> inputFormatted = this.getDataset("in").mapToPair(Node::fromCloudECString);

		
		
		Tuple2<JavaPairRDD<Long, Node>, JavaPairRDD<Long, String>> inOriMapped;
		inOriMapped = this.splitWithMapping(inputFormatted);
		
		/**
		 * Then, we run the task being tested.
		 */
		JavaPairRDD<Long, KmerCount> output = this.task.run(inOriMapped._1, this.getSplitStrategy());

		
		JavaPairRDD<String, KmerCount> outputMapped;
		outputMapped = this.joinWithMapping(output, inOriMapped._2);
		
		/**
		 * Finally, we format the output to the format used by the Hadoop version of
		 * CloudEC.
		 */
		JavaRDD<String> outputFormatted = outputMapped.flatMap(element -> {
			List<String> out = new LinkedList<>();
			for (Integer pos : element._2.count_P) {
				out.add(element._1 + "\tP\t" + pos.toString());
			}
			for (Integer pos : element._2.count_F) {
				out.add(element._1 + "\tF\t" + pos.toString());
			}
			return out.iterator();
		});

		assertRDDEquals(outputFormatted, this.getDataset("out"));
	}

}

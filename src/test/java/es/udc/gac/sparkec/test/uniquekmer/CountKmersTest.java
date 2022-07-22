/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
import es.udc.gac.sparkec.uniquekmer.CountKmers;
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

	private CountKmers task;

	public CountKmersTest() {
		super("uniquekmer_Countkmers", new String[] { "in", "out" });
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.task = new CountKmers(this.config.getK(), this.config.getJavaSparkContext());
	}

	/**
	 * Test of run method, of class CountKmers.
	 * 
	 * @throws es.udc.gac.sparkec.test.utils.DataSetNotFoundException
	 */
	@Test
	public void testRun() throws DataSetNotFoundException {
		JavaPairRDD<String, Node> in = this.getDataset("in").mapToPair(Node::fromCloudECString);

		JavaRDD<String> expectedOut = this.getDataset("out");

		Tuple2<JavaPairRDD<Long, Node>, JavaPairRDD<Long, String>> processedIn;
		processedIn = this.splitWithMapping(in);

		JavaPairRDD<Long, Integer> out = this.task.run(processedIn._1, this.getSplitStrategy());

		JavaPairRDD<String, Integer> mappedOut = this.joinWithMapping(out, processedIn._2);

		JavaPairRDD<String, String> expectedOutSplit;
		expectedOutSplit = expectedOut.mapToPair(e -> {
			String[] parts = e.split("\t", 2);

			return new Tuple2<>(parts[0], parts[1]);
		});

		JavaPairRDD<String, Iterable<String>> expectedOutGrouped;
		expectedOutGrouped = expectedOutSplit.groupByKey();

		JavaPairRDD<String, Integer> expectedOutFormatted;
		expectedOutFormatted = expectedOutGrouped.mapToPair(e -> {
			int i = 0;
			Iterator<String> it = e._2.iterator();
			while (it.hasNext()) {
				it.next();
				i++;
			}

			return new Tuple2<>(e._1, i);
		});

		JavaRDD<String> expectedOutJoined;
		expectedOutJoined = expectedOutFormatted.map(e -> {
			return e._1 + "\tV\t" + e._2;
		});

		JavaRDD<String> outJoined;
		outJoined = mappedOut.map(e -> {
			return e._1 + "\tV\t" + e._2;
		});

		assertRDDEquals(expectedOutJoined, outJoined);
	}

}

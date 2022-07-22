/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.udc.gac.sparkec.test.largekmer;

import static com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.junit.Before;
import org.junit.Test;

import es.udc.gac.sparkec.largekmer.KmerCount;
import es.udc.gac.sparkec.largekmer.TagReads;
import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.test.utils.DataSetNotFoundException;
import es.udc.gac.sparkec.test.utils.PhaseTest;
import scala.Tuple2;

/**
 *
 * @author marco
 */

public class TagReadsTest extends PhaseTest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private TagReads task;

	public TagReadsTest() {
		super("largekmer_TagReads", new String[] { "in_data", "in_kmers", "out" });
	}

	@Before
	@Override
	public void setUp() throws Exception {
		super.setUp();
		this.task = new TagReads(this.config.getJavaSparkContext(), this.config.getK());
	}

	@Test
	public void testRun() throws DataSetNotFoundException {
		/**
		 * First, we format the node input into the expected format.
		 */
		JavaPairRDD<String, Node> inputFormatted = this.getDataset("in_data").mapToPair(Node::fromCloudECString);

		/**
		 * We will try to regenerate the KmerCount struct given the raw data. In order
		 * to do that, we are first refactoring the input to the given format: Key:
		 * NodeID Value: IGN_Type + "\t" + position
		 */
		JavaPairRDD<String, String> refactoredKmerInput;
		refactoredKmerInput = this.getDataset("in_kmers").mapToPair(element -> {
			String[] parts = element.split("\t");
			return new Tuple2<>(parts[0], parts[1] + "\t" + parts[2]);
		});

		/**
		 * Then, we group this input data by nodes.
		 */
		JavaPairRDD<String, Iterable<String>> refactoredKmerInputGrouped;
		refactoredKmerInputGrouped = refactoredKmerInput.groupByKey();

		/**
		 * After this, we join this data with the node data.
		 */
		JavaPairRDD<String, Tuple2<Optional<Iterable<String>>, Node>> joinedKmerInputData;
		joinedKmerInputData = refactoredKmerInputGrouped.rightOuterJoin(inputFormatted);

		/**
		 * Then, we output the data in the expected format.
		 */
		JavaPairRDD<String, KmerCount> kmerInputData;
		kmerInputData = joinedKmerInputData.mapToPair(element -> {
			KmerCount out = new KmerCount();

			out.node = element._2._2;
			if (element._2._1.isPresent()) {
				Iterable<String> ignList = element._2._1.get();
				for (String ign : ignList) {
					// 0 = IGNType || 1 = IGNPos
					String[] parts = ign.split("\t");
					if (parts[0].equals("F")) {
						out.count_F.add(Integer.parseInt(parts[1]));
					} else if (parts[0].equals("P")) {
						out.count_P.add(Integer.parseInt(parts[1]));
					}
				}
			}

			return new Tuple2<>(element._1, out);
		});

		Tuple2<JavaPairRDD<Long, Node>, JavaPairRDD<Long, String>> inOriMapped;
		inOriMapped = this.splitWithMapping(inputFormatted);

		JavaPairRDD<Long, KmerCount> kmerInputDataMapped;
		kmerInputDataMapped = this.splitUsingMapping(kmerInputData, inOriMapped._2);

		/**
		 * Afterwards, we run the task being tested.
		 */
		JavaPairRDD<Long, Node> output = this.task.run(kmerInputDataMapped);

		JavaPairRDD<String, Node> outputMapped = this.joinWithMapping(output, inOriMapped._2);

		JavaRDD<String> expectedOut = this.getDataset("out");

		JavaPairRDD<String, Node> expectedOutProcessed = expectedOut.mapToPair(Node::fromCloudECString);

		assertRDDEquals(outputMapped.map(e -> e._2.toCloudECString(e._1)),
				expectedOutProcessed.map(e -> e._2.toCloudECString(e._1)));

	}

}

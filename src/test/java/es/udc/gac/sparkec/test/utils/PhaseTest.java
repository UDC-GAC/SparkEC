
package es.udc.gac.sparkec.test.utils;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import es.udc.gac.sparkec.Config;
import es.udc.gac.sparkec.split.IPhaseSplitStrategy;
import es.udc.gac.sparkec.split.PhaseSplitStrategy;
import scala.Tuple2;

public abstract class PhaseTest extends SharedJavaSparkContext implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private class DataSetData {
		private boolean loaded;
		private JavaRDD<String> dataSet;
		private String filePath;

		private JavaRDD<String> getDataSet() throws DataSetNotFoundException {
			if (!this.loaded) {
				try {
					this.dataSet = config.getJavaSparkContext().textFile(this.filePath);
				} catch (Exception e) {
					throw new DataSetNotFoundException(this.filePath);
				}
				this.loaded = true;
			}
			if (this.dataSet == null) {
				throw new DataSetNotFoundException(this.filePath);
			}

			return this.dataSet;
		}

		public DataSetData(String filePath) {
			this.loaded = false;
			this.dataSet = null;
			this.filePath = filePath;
		}

	};

	private static final String DATASET_BASE_PATH = "DataSets/";

	private Map<String, DataSetData> dataSets = new HashMap<>();

	private IPhaseSplitStrategy splitStrategy = null;

	protected IPhaseSplitStrategy getSplitStrategy() {
		if (splitStrategy == null) {
			splitStrategy = new PhaseSplitStrategy(24, 4L * 1024L * 1024L * 1024L, 5.25f);
			splitStrategy.initialize(100, 7000);
		}

		return splitStrategy;
	}

	protected JavaRDD<String> getDataset(String name) throws DataSetNotFoundException {
		DataSetData tmp = dataSets.get(name);
		if (tmp == null) {
			throw new DataSetNotFoundException(name);
		}
		return tmp.getDataSet();
	}

	protected Config config;
	protected final String phaseName;
	private final String[] dataSetsNames;

	protected PhaseTest(String phaseName, String[] dataSetsNames) {
		this.phaseName = phaseName;
		this.dataSetsNames = dataSetsNames;
	}

	@Before
	protected void setUp() throws Exception {

		this.config = null;
		try {
			this.config = new Config(this.jsc());
		} catch (IOException e) {
			fail();
		}

		for (String dataSetName : this.dataSetsNames) {
			this.dataSets.put(dataSetName,
					new DataSetData(new File(getClass().getClassLoader()
							.getResource(DATASET_BASE_PATH + this.phaseName + "/" + dataSetName + ".txt").getFile())
									.getAbsolutePath()));
		}
		this.config.readConfig(new File(
				getClass().getClassLoader().getResource("config/" + phaseName + "/config.properties").getFile())
						.getAbsolutePath());

	}

	protected String getDatasetBasePath() {
		return new File(getClass().getClassLoader().getResource(DATASET_BASE_PATH + this.phaseName).getFile())
				.getAbsolutePath() + "/";
	}

	protected <T> Tuple2<JavaPairRDD<Long, T>, JavaPairRDD<Long, String>> splitWithMapping(
			JavaPairRDD<String, T> nodes) {

		JavaPairRDD<Tuple2<String, T>, Long> zipped;
		zipped = nodes.zipWithUniqueId();

		JavaPairRDD<Long, String> mappings;
		mappings = zipped.mapToPair(e -> new Tuple2<>(e._2, e._1._1));

		JavaPairRDD<Long, T> resultingNodes;
		resultingNodes = zipped.mapToPair(e -> new Tuple2<>(e._2, e._1._2));

		return new Tuple2<>(resultingNodes, mappings);
	}

	protected <T> JavaPairRDD<String, T> joinWithMapping(JavaPairRDD<Long, T> nodes,
			JavaPairRDD<Long, String> mappings) {
		return nodes.join(mappings).mapToPair(e -> new Tuple2<>(e._2._2, e._2._1));
	}

	protected <T> JavaPairRDD<Long, T> splitUsingMapping(JavaPairRDD<String, T> ori,
			JavaPairRDD<Long, String> mappings) {
		return ori.join(mappings.mapToPair(e -> new Tuple2<>(e._2, e._1)))
				.mapToPair(e -> new Tuple2<>(e._2._2, e._2._1));
	}

}

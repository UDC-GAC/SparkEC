package es.udc.gac.sparkec.preprocess;

import java.io.Serializable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import es.udc.gac.hadoop.sequence.parser.mapreduce.FastAInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.FastQInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.SingleEndSequenceRecordReader;
import es.udc.gac.sparkec.node.Node;

/**
 * Internal class used to parse the format returned by Hadoop Sequence Parser into
 * the Node representation for the Sequences.
 */
class PreProcessWorker implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Total accumulator.
	 */
	private LongAccumulator totals;

	/**
	 * Invalid accumulator.
	 */
	private LongAccumulator invalids;

	/**
	 * Default constructor for the PreProcessWorker.
	 * @param totals Total accumulator
	 * @param invalids Invalid accumulator
	 */
	public PreProcessWorker(LongAccumulator totals, LongAccumulator invalids) {

		this.totals = totals;
		this.invalids = invalids;
	}

	/**
	 * Runs the worker.
	 * @param input The text of the input
	 * @return Tuple containing both the node ID, and the Node itself
	 */
	JavaPairRDD<String, Node> run(JavaPairRDD<LongWritable, Text> input) {

		return input.mapToPair(e -> {
			return Node.fromSFQString(SingleEndSequenceRecordReader.getRead(e._2).split("\n"));
		});
	}

	/**
	 * Returns the total value of the total accumulator.
	 * @return The total value of the total accumulator
	 */
	public long getTotals() {
		return this.totals.value();
	}

	/**
	 * Returns the total value of the invalid accumulator.
	 * @return The total value of the invalid accumulator
	 */
	public long getInvalids() {
		return this.invalids.value();
	}

	/**
	 * Clears the value of the total accumulator.
	 */
	public void clearTotals() {
		this.totals.setValue(0L);
	}

	/**
	 * Clears the value of the invalid accumulator.
	 */
	public void clearInvalids() {
		this.totals.setValue(0L);
	}
}

/**
 * SubPhase of the PreProcess Phase. This SubPhase takes care of reading the input dataset from either
 * HDFS or the local file system. In order to read the data, it might use the Hadoop Sequence Parser library.
 */
public class PreProcessTransform implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Format used by the input dataset.
	 */
	private final String inputType;

	/**
	 * The JavaSparkContext.
	 */
	private transient JavaSparkContext jsc;

	/**
	 * The worker needed to process the Hadoop Sequence Parser output.
	 */
	private PreProcessWorker worker;

	/**
	 * Default constructor for PreProcessTransform.
	 * @param inputType The format used by the input dataset
	 * @param jsc The JavaSparkContext
	 * @param totals The totals accumulator
	 * @param invalids The invalid accumulator
	 */
	public PreProcessTransform(String inputType, JavaSparkContext jsc, LongAccumulator totals,
			LongAccumulator invalids) {
		this.inputType = inputType;
		this.jsc = jsc;
		this.worker = new PreProcessWorker(totals, invalids);
	}

	/**
	 * Runs this SubPhase. 
	 * @param path The path of the input data
	 * @return Tuple containing the Node ID and the Node itself.
	 */
	public JavaPairRDD<String, Node> run(String path) {
		Class<? extends FileInputFormat<LongWritable, Text>> parser = null;

		switch (inputType) {

		case "FastQ":
			parser = FastQInputFormat.class;
			break;

		case "FastA":
			parser = FastAInputFormat.class;
			break;
		case "internal":
		default:
			return this.jsc.textFile(path).mapToPair(Node::fromSFQString);

		}

		JavaPairRDD<LongWritable, Text> input;
		input = this.jsc.newAPIHadoopFile(path, parser, LongWritable.class, Text.class, this.jsc.hadoopConfiguration());

		return this.worker.run(input);
	}

	/**
	 * Returns the total value of the total accumulator.
	 * @return The total value of the total accumulator
	 */
	public long getTotals() {
		return this.worker.getTotals();
	}

	/**
	 * Returns the total value of the invalid accumulator.
	 * @return The total value of the invalid accumulator
	 */
	public long getInvalids() {
		return this.worker.getInvalids();
	}

	/**
	 * Clears the value of the total accumulator.
	 */
	public void clearTotals() {
		this.worker.clearTotals();
	}

	/**
	 * Clears the value of the invalid accumulator.
	 */
	public void clearInvalids() {
		this.worker.clearInvalids();
	}
}

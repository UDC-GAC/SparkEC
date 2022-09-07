package es.udc.gac.sparkec;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Container for the configuration being used by SparkEC.
 */
public class Config {

	private static final Logger logger = LogManager.getLogger();

	// FRAMEWORK
	/**
	 * The JavaSparkContext.
	 */
	private final JavaSparkContext jsc;

	/**
	 * The SparkConf.
	 */
	private final SparkConf sc;

	/**
	 * Reference to the HDFS file system.
	 */
	private final FileSystem fs;

	/**
	 * Spark log level.
	 */
	private String sparkLogLevel = "WARN";

	// GLOBAL
	/**
	 * The k being used for this execution.
	 */
	private final Integer k = 24;

	/**
	 * The average sequence length (hint).
	 */
	private final Integer seqLen = 0;

	/**
	 * Number of input sequences (hint).
	 */
	private final Long numSeq = 0L;

	/**
	 * The C constant parameter used for the memory estimation of the split-based system.
	 */
	private final Float splitMemoryConstant = 5.25f;

	/**
	 * The partition size in MiB
	 */
	private final Integer partitionSize = 4;

	// PREPROCESS
	/**
	 * The format of the input dataset.
	 */
	private String inputType = "FastQ";

	/**
	 * Filter (ignore) reads have N bases from input.
	 */
	private final Boolean shaveIgnore = true;

	// PINCH_CORRECT
	/**
	 * Number of passes to make for PinchCorrect.
	 */
	private final Integer numPinchCorrectAttempts = 1;

	// LARGE_KMER_FILTER
	/**
	 * Whether the PinchCorrect filter should be run.
	 */
	private final Boolean filter_P = false;

	/**
	 * Whether the SpreadCorrect filter should be run.
	 */
	private final Boolean filter_S = true;

	// SPREAD_CORRECT

	/**
	 * Number of passes to make for SpreadCorrect.
	 */
	private final Integer numSpreadCorrectAttempts = 1;

	/**
	 * The arm size to use for SpreadCorrect. Higher numbers lead to higher memory usage, and best
	 * quality results. Use -1 as a flag to use the higher number possible.
	 */
	private final Integer arm = -1; // -1 to use the entire reads as arm

	/**
	 * The height to use for SpreadCorrect.
	 */
	private final Integer height = 0;

	/**
	 * The arm scheme to run for SpreadCorrect.
	 */
	private final String scheme = "";

	/**
	 * The maximum stack to use for SpreadCorrect.
	 */
	private final Integer stackMax = -1; // -1 to disable

	/**
	 * The minimum stack to use for SpreadCorrect.
	 */
	private final Integer stackMin = -1; // -1 to disable

	// POSTPROCESS
	/**
	 * Merge ignored reads from input.
	 */
	private final Boolean mergeIgnore = false;

	// OUTPUT TEMPORAL STAGES
	/**
	 * Whether to output the PreProcess result as a temporary result.
	 */
	private final Boolean outputPreprocess = false;

	/**
	 * Whether to output the PinchCorrect result as a temporary result.
	 */
	private final Boolean outputPinchCorrect = false;

	/**
	 * Whether to output the LargeKmerFilter result as a temporary result.
	 */
	private final Boolean outputLargeKmerFilter = false;

	/**
	 * Whether to output the SpreadCorrect result as a temporary result.
	 */
	private final Boolean outputSpreadCorrect = false;

	/**
	 * Whether to output the UniqueKmerFilter result as a temporary result.
	 */
	private final Boolean outputUniqueKmerFilter = false;

	// ENABLE STAGES
	/**
	 * Whether to enable the PinchCorrect phase.
	 */
	private final Boolean enablePinchCorrect = true;

	/**
	 * Whether to enable the LargeKmerFilter phase.
	 */
	private final Boolean enableLargeKmerFilter = true;

	/**
	 * Whether to enable the SpreadCorrect phase.
	 */
	private final Boolean enableSpreadCorrect = true;

	/**
	 * Whether to enable the UniqueKmerFilter phase.
	 */
	private final Boolean enableUniqueKmerFilter = true;

	// KRYO_ENABLED

	/**
	 * Whether the Kryo serialization library is being used.
	 */
	private boolean isKryoEnabled = false;

	/**
	 * Returns whether the PinchCorrect phase is enabled.
	 * @return Whether the PinchCorrect phase is enabled
	 */
	public boolean isEnablePinchCorrect() {
		return enablePinchCorrect;
	}

	/**
	 * Returns whether the LargeKmerFilter phase is enabled.
	 * @return Whether the LargeKmerFilter phase is enabled
	 */
	public boolean isEnableLargeKmerFilter() {
		return enableLargeKmerFilter;
	}

	/**
	 * Returns whether the SpreadCorrect phase is enabled.
	 * @return Whether the SpreadCorrect phase is enabled
	 */
	public boolean isEnableSpreadCorrect() {
		return enableSpreadCorrect;
	}

	/**
	 * Returns whether the UniqueKmerFilter phase is enabled.
	 * @return Whether the UniqueKmerFilter phase is enabled
	 */
	public boolean isEnableUniqueKmerFilter() {
		return enableUniqueKmerFilter;
	}

	/**
	 * Returns whether the temporary output of PreProcess is enabled.
	 * @return Whether the temporary output of PreProcess is enabled
	 */
	public boolean isOutputPreprocess() {
		return outputPreprocess;
	}

	/**
	 * Returns whether the temporary output of PinchCorrect is enabled.
	 * @return Whether the temporary output of PinchCorrect is enabled
	 */
	public boolean isOutputPinchCorrect() {
		return outputPinchCorrect;
	}

	/**
	 * Returns whether the temporary output of LargeKmerFilter is enabled.
	 * @return Whether the temporary output of LargeKmerFilter is enabled
	 */
	public boolean isOutputLargeKmerFilter() {
		return outputLargeKmerFilter;
	}

	/**
	 * Returns whether the temporary output of SpreadCorrect is enabled.
	 * @return Whether the temporary output of SpreadCorrect is enabled
	 */
	public boolean isOutputSpreadCorrect() {
		return outputSpreadCorrect;
	}

	/**
	 * Returns whether the temporary output of UniqueKmerFilter is enabled.
	 * @return Whether the temporary output of UniqueKmerFilter is enabled
	 */
	public boolean isOutputUniqueKmerFilter() {
		return outputUniqueKmerFilter;
	}

	/**
	 * Returns whether the Kryo serialization library is enabled.
	 * @return Whether the Kryo serialization library is enabled
	 */
	public boolean isKryoEnabled() {
		return isKryoEnabled;
	}

	/**
	 * Sets whether the Kryo serialization library is enabled.
	 * @param value Whether the Kryo serialization library is enabled
	 */
	public void setKryoEnabled(boolean value) {
		this.isKryoEnabled = value;
	}

	/**
	 * Gets the Java Spark Context
	 * @return The JavaSparkContext
	 */
	public JavaSparkContext getJavaSparkContext() {
		return jsc;
	}

	public String getSparkLogLevel() {
		return sparkLogLevel;
	}

	/**
	 * Gets the K being used by this execution.
	 * @return The k being used by this execution
	 */
	public int getK() {
		return k;
	}

	public int getSeqLen() {
		return seqLen;
	}

	public long getNumSeq() {
		return numSeq;
	}

	/**
	 * Gets the partition size being used by this execution.
	 * @return The partition size being used by this execution
	 */
	public int getPartitionSize() {
		return partitionSize;
	}

	/**
	 * Gets the constant value used for the memory estimation
	 * of the split-based system.
	 * @return The constant value
	 */
	public Float getSplitMemoryConstant() {
		return splitMemoryConstant;
	}

	/**
	 * Gets the input format being used by this execution.
	 * @return The input format being used by this execution
	 */
	public String getInputType() {
		return inputType;
	}

	/**
	 * Gets whether the LargeKmerFilter for PinchCorrect is enabled.
	 * @return Whether the LargeKmerFilter for PinchCorrect is enabled
	 */
	public boolean getFilter_P() {
		return filter_P;
	}

	/**
	 * Gets whether the LargeKmerFilter for SpreadCorrect is enabled.
	 * @return Whether the LargeKmerFilter for SpreadCorrect is enabled
	 */
	public boolean getFilter_S() {
		return filter_S;
	}

	/**
	 * Gets the number of passes to make for PinchCorrect.
	 * @return The number of passes to make for PinchCorrect
	 */
	public int getNumPinchCorrectAttempts() {
		return numPinchCorrectAttempts;
	}

	/**
	 * Gets the number of passes to make for SpreadCorrect.
	 * @return The number of passes to make for SpreadCorrect
	 */
	public int getNumSpreadCorrectAttempts() {
		return this.numSpreadCorrectAttempts;
	}

	/**
	 * Gets the arm size to use for SpreadCorrect.
	 * @return The arm size to use for SpreadCorrect
	 */
	public int getArm() {
		return arm;
	}

	/**
	 * Gets the height to use for SpreadCorrect.
	 * @return The height to use for SpreadCorrect
	 */
	public int getHeight() {
		return height;
	}

	/**
	 * Gets the scheme to use for SpreadCorrect.
	 * @return The scheme to use for SpreadCorrect
	 */
	public String getScheme() {
		return scheme;
	}

	/**
	 * Gets the maximum number of kmers to use for each alignment at SpreadCorrect.
	 * @return The maximum number of kmers to use for each alignment at SpreadCorrect
	 */
	public int getStackMax() {
		return stackMax;
	}

	/**
	 * Gets the minimum number of kmers to use for each alignment at SpreadCorrect.
	 * @return The minimum number of kmers to use for each alignment at SpreadCorrect
	 */
	public int getStackMin() {
		return stackMin;
	}

	/**
	 * Gets mergeIgnore.
	 * @return Whether to merge ignored reads from input
	 */
	public boolean getMergeIgnore() {
		return mergeIgnore;
	}

	/**
	 * Gets shaveIgnore.
	 * @return Whether to filter (ignore) reads have N bases from input
	 */
	public boolean getShaveIgnore() {
		return shaveIgnore;
	}

	/**
	 * Checks whether a given file exists into HDFS.
	 * @param path The file path
	 * @return Whether the file exists
	 * @throws IOException If an internal IOException happened when checking if the file existed
	 */
	public boolean HDFSFileExists(String path) throws IOException {
		Path p = new Path(path);
		return fs.exists(p);
	}

	/**
	 * Deletes a given file from HDFS.
	 * @param path The path of the file to delete
	 * @throws IOException If an internal IOException happened when deleting the file
	 */
	public void deleteFile(String path) throws IOException {
		Path p = new Path(path);
		fs.delete(p, true);
	}

	/**
	 * Default empty constructor for the Config class
	 * @throws IOException If the HDFS file system could not be set.
	 */
	public Config() throws IOException {
		this.sc = new SparkConf();
		SparkContext aux = SparkContext.getOrCreate(sc);
		this.jsc = JavaSparkContext.fromSparkContext(aux);
		this.fs = FileSystem.get(jsc.hadoopConfiguration());
	}

	/**
	 * Default constructor for the Config class using a JavaSparkContext
	 * @param jsc The JavaSparkContext to use
	 * @throws IOException If the HDFS file system could not be set.
	 */
	public Config(JavaSparkContext jsc) throws IOException {
		this.jsc = jsc;
		this.sc = jsc.getConf();
		this.fs = FileSystem.get(jsc.hadoopConfiguration());
	}

	/**
	 * Prints to the default logger output the current configuration being used.
	 */
	public void printConfig() {
		try {
			List<Field> banneds;
			banneds = Arrays.asList(
					new Field[] { this.getClass().getDeclaredField("jsc"), this.getClass().getDeclaredField("sc"),
							this.getClass().getDeclaredField("fs"), this.getClass().getDeclaredField("logger") });

			Field[] fields = this.getClass().getDeclaredFields();
			for (Field f : fields) {
				if (!banneds.contains(f)) {
					logger.info(String.format("\t%s: %s", f.getName(), f.get(this).toString()));
				}
			}
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			logger.fatal("Error showing config file");
			System.exit(-1);
		}
	}

	/**
	 * Parses a configuration file, setting all the fields of this instance to the specified values.
	 * @param filePath The configurarion file path
	 */
	public void readConfig(String filePath) {
		Properties configFile;
		configFile = new Properties();
		try {

			InputStream stream = new FileInputStream(filePath);
			configFile.load(stream);

			List<Field> banneds;
			banneds = Arrays.asList(
					new Field[] { this.getClass().getDeclaredField("jsc"), this.getClass().getDeclaredField("sc"),
							this.getClass().getDeclaredField("fs"), this.getClass().getDeclaredField("logger") });

			Field[] fields = this.getClass().getDeclaredFields();
			for (Field f : fields) {
				if (!banneds.contains(f)) {
					String v = configFile.getProperty(f.getName());
					if (v != null) {
						f.setAccessible(true);
						if (f.getType() == String.class) {
							f.set(this, v);
						} else if (f.getType() == Boolean.class) {
							f.set(this, Boolean.parseBoolean(v));
						} else if (f.getType() == Integer.class) {
							f.set(this, Integer.parseInt(v));
						} else if (f.getType() == Long.class) {
							f.set(this, Long.parseLong(v));
						}
					}
				}
			}

		} catch (IOException | NoSuchFieldException | SecurityException | IllegalArgumentException
				| IllegalAccessException e) {
			logger.fatal("Error parsing config file");
			System.exit(-1);
		}
	}
}

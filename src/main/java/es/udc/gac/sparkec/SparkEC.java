package es.udc.gac.sparkec;

import static java.lang.System.exit;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import es.udc.gac.sparkec.largekmer.LargeKmerFilter;
import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.pinchcorrect.PinchCorrect;
import es.udc.gac.sparkec.postprocess.PostProcess;
import es.udc.gac.sparkec.preprocess.PreProcess;
import es.udc.gac.sparkec.sequence.EagerDNASequence;
import es.udc.gac.sparkec.sequence.LazyDNASequence;
import es.udc.gac.sparkec.spreadcorrect.SpreadCorrect;
import es.udc.gac.sparkec.uniquekmer.UniqueKmerFilter;

/**
 * Main entrypoint class for the SparkEC error correction system. This class will handle the execution
 * of all the phases of the system, calling each phase when needed.
 */
public class SparkEC {

	private static final String GLOBAL_MEASURE_NAME = "Global";
	private static final String OUTPUT_MEASURE_NAME = "Output";

	public static final String WEBPAGE = "https://github.com/UDC-GAC/SparkEC";
	public static final String VERSION = "v1.0.1";

	private static Logger logger;

	/**
	 * List of the phases being run in this execution of SparkEC
	 */
	private List<Phase> phases;

	/**
	 * The Spark Context of the current execution
	 */
	private JavaSparkContext jsc;

	/**
	 * Container to handle the storage of all the necessary datasets for each phase and the stats
	 * for the datasets.
	 */
	private Data data;

	/**
	 * The configuration being currently used in this execution.
	 */
	private Config config;

	// Paths

	/**
	 * The input path being used by the system. It might be HDFS or a local file system path.
	 */
	private String inputPath;

	/**
	 * The output path being used by the system. It must be HDFS.
	 */
	private String outputPath;

	/**
	 * The configuration path being used by the system. It must be a local file system path.
	 */
	private String configPath;

	/**
	 * Default entrypoint of the SparkEC system.
	 * @param args The command line arguments
	 */
	public static void main(String[] args) {
		System.setProperty("log4j.configurationFile", "log4j2.xml");
		SparkEC.logger = LogManager.getLogger();

		SparkEC mainObject = new SparkEC(args);
		mainObject.run();
	}

	/**
	 * Parses the command line arguments of the SparkEC system.
	 * @param args The command line arguments
	 */
	private void parseArgs(String[] args) {
		boolean in = false;
		boolean out = false;
		boolean configFound = false;

		for (int i = 0; i < args.length - 1; i++) {
			if (args[i].equals("-in")) {
				if (in) {
					logger.error("Multiple inputs");
					exit(-1);
				}
				in = true;
				this.inputPath = args[i + 1];
			}
			if (args[i].equals("-out")) {
				if (out) {
					logger.error("Multiple outputs");
					exit(-1);
				}
				out = true;
				this.outputPath = args[i + 1];
			}
			if (args[i].equals("-config")) {
				if (configFound) {
					logger.error("Multiple config files");
					exit(-1);
				}
				configFound = true;
				this.configPath = args[i + 1];
			}
		}
		if (!in || !out) {
			logger.error("Usage: spark-submit SparkEC.jar -in <input> -out <output> [-config <config>]");
			logger.info("More information available at: "+WEBPAGE+"\n\n");
			exit(-1);
		}
	}

	/**
	 * Auxiliary method to determine all the cluster available memory for RDD storage purposes
	 * @return The number of available memory bytes for RDD storage
	 */
	private long determineAvailableMemory() {
		SparkConf conf = jsc.sc().conf();

		boolean isLocal = conf.get("spark.master").startsWith("local");

		String memoryString;
		int executorCount;

		if (isLocal) {
			executorCount = 1;
			memoryString = conf.get("spark.driver.memory", "1g").toLowerCase();
		} else {
			executorCount = conf.getInt("spark.executor.instances", 1);
			memoryString = conf.get("spark.executor.memory", "1g").toLowerCase();
		}
		long executorMemory;
		switch (memoryString.charAt(memoryString.length() - 1)) {
		case 'k':
			executorMemory = Long.parseLong(memoryString.substring(0, memoryString.length() - 1)) * 1024;
			break;
		case 'm':
			executorMemory = Long.parseLong(memoryString.substring(0, memoryString.length() - 1)) * 1024 * 1024;
			break;
		case 'g':
			executorMemory = Long.parseLong(memoryString.substring(0, memoryString.length() - 1)) * 1024 * 1024 * 1024;
			break;
		case 't':
			executorMemory = Long.parseLong(memoryString.substring(0, memoryString.length() - 1)) * 1024 * 1024 * 1024 * 1024;
			break;
		default:
			executorMemory = Long.parseLong(memoryString.substring(0, memoryString.length() - 1));
			break;
		}


		return executorMemory * executorCount;
	}

	/**
	 * Default constructor for the SparkEC controller class.
	 * @param args The command line arguments passed to the system
	 */
	private SparkEC(String[] args) {
		try {

			parseArgs(args);

			this.config = new Config();
			this.jsc = config.getJavaSparkContext();
			jsc.setLogLevel("WARN");

			if (!config.HDFSFileExists(inputPath)) {
				logger.error("Invalid input path");
				exit(-1);
			}

			if (config.HDFSFileExists(outputPath)) {
				config.deleteFile(outputPath);
			}

			if (config.HDFSFileExists(outputPath + "_tmp")) {
				config.deleteFile(outputPath + "_tmp");
			}

			if (this.configPath != null) {
				config.readConfig(configPath);
			}

			String sparkSerializer = jsc.sc().conf().get("spark.serializer", "org.apache.spark.serializer.JavaSerializer");

			config.setKryoEnabled(sparkSerializer.equals("org.apache.spark.serializer.KryoSerializer"));

			long memoryAvailable = determineAvailableMemory();

			data = new Data(outputPath, outputPath + "_tmp", memoryAvailable, config.getSplitMemoryConstant(), config.getK());
			phases = new LinkedList<>();

			phases.add(new PreProcess(config, inputPath));

			if (config.isEnablePinchCorrect())
				phases.add(new PinchCorrect(config));

			if (config.isEnableLargeKmerFilter())
				phases.add(new LargeKmerFilter(config));

			if (config.isEnableSpreadCorrect())
				phases.add(new SpreadCorrect(config));

			if (config.isEnableUniqueKmerFilter())
				phases.add(new UniqueKmerFilter(config));

			phases.add(new PostProcess(config));

		} catch (Exception e) {
			logger.fatal("Message: " + e.getLocalizedMessage());
			exit(-1);
		}

	}

	/**
	 * Runs the SparkEC error correction system.
	 */
	private void run() {
		try {
			TimeMonitor timeMonitor = new TimeMonitor();
			timeMonitor.startMeasuring(SparkEC.GLOBAL_MEASURE_NAME);

			logger.info("Starting SparkEC "+VERSION);
			logger.info(String.format("Input path: %s", this.inputPath));
			logger.info(String.format("Output path: %s", this.outputPath));
			if (this.configPath != null) {
				logger.info(String.format("Config path: %s", this.configPath));
			}
			logger.info("CONFIG:");
			config.printConfig();

			if (config.isKryoEnabled()) {
				config.getJavaSparkContext().sc().conf()
				.registerKryoClasses(new Class[] { Node.class, LazyDNASequence.class, EagerDNASequence.class });
			}

			for (Phase p : phases) {
				logger.info("Computing phase: " + p.getPhaseName());
				timeMonitor.startMeasuring(p.getPhaseName());
				p.runPhase(data);
				logger.info("Phase succesfully computed");
				timeMonitor.finishMeasuring(p.getPhaseName());
			}

			timeMonitor.startMeasuring(OUTPUT_MEASURE_NAME);
			data.outputData();
			timeMonitor.finishMeasuring(OUTPUT_MEASURE_NAME);

			timeMonitor.finishMeasuring(SparkEC.GLOBAL_MEASURE_NAME);

			logger.info(" ");
			logger.info("SparkEC finished succesfully");
			logger.info("Phase stats: ");
			for (Phase p : phases) {
				logger.info(String.format("\t%s", p.getPhaseName()));
				p.printStats();
			}

			logger.info(" ");
			logger.info("Phase times: ");

			Iterator<Map.Entry<String, Float>> it;
			it = timeMonitor.iterator();

			List<String> timesFormatted = new LinkedList<>();
			float phasesTotal=0.0f;
			while (it.hasNext()) {
				Map.Entry<String, Float> e = it.next();
				if (e.getKey().equals(SparkEC.GLOBAL_MEASURE_NAME) || e.getKey().equals(SparkEC.OUTPUT_MEASURE_NAME)) {
					continue;
				}
				phasesTotal+=e.getValue();
				timesFormatted.add("\t" + e.getKey() + ": " + String.format("%.4f", e.getValue()) + "s");
			}
			Collections.sort(timesFormatted);
			timesFormatted.forEach(e -> logger.info(e));

			logger.info(
					String.format("All phases: %.4fs", phasesTotal));
			logger.info(
					String.format("Output data: %.4fs", timeMonitor.getMeasurement(SparkEC.OUTPUT_MEASURE_NAME)));
			logger.info(
					String.format("Elapsed execution time: %.4fs", timeMonitor.getMeasurement(SparkEC.GLOBAL_MEASURE_NAME)));

		} catch (Exception e) {
			logger.fatal("Message: " + e.getMessage());
			exit(-1);
		}
	}
}

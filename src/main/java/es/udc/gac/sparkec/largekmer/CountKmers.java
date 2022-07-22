package es.udc.gac.sparkec.largekmer;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.sequence.IDNASequence;
import es.udc.gac.sparkec.sequence.UnableToSkipMiddleBase;
import es.udc.gac.sparkec.split.IPhaseSplitStrategy;
import scala.Tuple2;

/**
 * CountKmers is a SubPhase of the LargeKmerFilter Phase. This SubPhase will emit kmers and generate 
 * ignore data ready to be attached to the nodes.
 */
public class CountKmers implements Serializable {


	private static final long serialVersionUID = 1L;

	private static final Logger logger = LogManager.getLogger();
	
	/**
	 * The k currently being used by this execution.
	 */
	private final int k;
	
	/**
	 * Whether the PinchCorrect filter should be run.
	 */
	private final boolean filter_P;
	
	/**
	 * Whether the SpreadCorrect filter should be run.
	 */
	private final boolean filter_S;

	/**
	 * The maximum number of reads for each kmer.
	 */
	private final int stackMax;
	
	/**
	 * The minimum number of read for each kmer.
	 */
	private final int stackMin;

	/**
	 * The HKmer accumulator.
	 */
	private final LongAccumulator hkmer;
	
	/**
	 * The LKmer accumulator.
	 */
	private final LongAccumulator lkmer;
	
	/**
	 * The split multiplier to use for this Phase.
	 */
	private static final int SPLIT_MULTIPLIER = 1;

	/**
	 * Returns the total value of the HKmer accumulator.
	 * @return The total value of the HKmer accumulator
	 */
	public long getHkmer() {
		return hkmer.value();
	}

	/**
	 * Returns the total value of the LKmer accumulator.
	 * @return The total value of the LKmer accumulator
	 */
	public long getLkmer() {
		return lkmer.value();
	}

	/**
	 * Emit kmers for each node
	 * 
	 * @param line       The node
	 * @param k          The k used by CloudEC
	 * @param splitIndex The current split being computed
	 * @param maxSplits  The number of splits being computed
	 * @param splitPrime The split prime being used for distribution
	 * @param splitStrategy The split strategy currently being used
	 * @param filterP    Whether kmers for PinchCorrect should be emitted
	 * @param filterS    Whether kmers for SpreadCorrect should be emitted
	 * @return The list with the kmers generated, and the ID and position of the
	 *         node where the kmer was found
	 */
	public static Iterable<Tuple2<KmerIgnoreData, Tuple2<Long, Integer>>> emitKmers(Tuple2<Long, Node> line, int k,
			int splitIndex, int maxSplits, int splitPrime, IPhaseSplitStrategy splitStrategy, boolean filterP, boolean filterS) {

		List<Tuple2<KmerIgnoreData, Tuple2<Long, Integer>>> out = new LinkedList<>();

		Node node = line._2;

		IDNASequence nodeStr = node.getSeq();

		// generate (k+1)-mers for PinchCorrect
		if (filterP) {
			int end = nodeStr.length() - k;
			for (int i = 0; i < end; i++) {

				IDNASequence window_f;
				try {
					window_f = nodeStr.getSubSequenceWithoutMiddleBase(i, k + i);
				} catch (UnableToSkipMiddleBase e) {
					throw new RuntimeException(e);
				}

				boolean invalidKmer = window_f.containsBase((byte) 'N');
				if (invalidKmer) {
					continue;
				}

				IDNASequence window_r = window_f.rcSequence(false);

				boolean reversed = window_f.compareTo(window_r) > 0;

				if (!reversed) {
					if (splitIndex != splitStrategy.computeItemSplitIndex(window_f, maxSplits, splitPrime)) {
						continue;
					}
					out.add(new Tuple2<KmerIgnoreData, Tuple2<Long, Integer>>(new KmerIgnoreData(true, window_f),
							new Tuple2<Long, Integer>(line._1, i)));
				} else {
					if (splitIndex != splitStrategy.computeItemSplitIndex(window_r, maxSplits, splitPrime)) {
						continue;
					}
					out.add(new Tuple2<KmerIgnoreData, Tuple2<Long, Integer>>(new KmerIgnoreData(true, window_r),
							new Tuple2<Long, Integer>(line._1, i)));
				}
			}
		}

		// generate k-mers for FindError
		if (filterS) {
			int end = nodeStr.length() - k + 1;
			for (int i = 0; i < end; i++) {

				IDNASequence window_f = node.getSeq().getSubSequence(i, i + k - 1);

				boolean invalidKmer = window_f.containsBase((byte) 'N');
				if (invalidKmer) {
					continue;
				}

				IDNASequence window_r = window_f.rcSequence();

				boolean reversed = window_f.compareTo(window_r) > 0;

				if (!reversed) {
					if (splitIndex != splitStrategy.computeItemSplitIndex(window_f, maxSplits, splitPrime)) {
						continue;
					}
					out.add(new Tuple2<KmerIgnoreData, Tuple2<Long, Integer>>(new KmerIgnoreData(false, window_f),
							new Tuple2<Long, Integer>(line._1, i)));
				} else {
					if (splitIndex != splitStrategy.computeItemSplitIndex(window_r, maxSplits, splitPrime)) {
						continue;
					}
					out.add(new Tuple2<KmerIgnoreData, Tuple2<Long, Integer>>(new KmerIgnoreData(false, window_r),
							new Tuple2<Long, Integer>(line._1, i)));
				}
			}
		}

		return out;
	}

	/**
	 * Filter the kmer who has been referenced by too much or too few nodes
	 * 
	 * @param kmer     The kmer
	 * @param stackMin Minimum number of reads to check (or -1 if you want to skip
	 *                 this check)
	 * @param stackMax Maximum number of reads to check (or -1 if you want to skip
	 *                 this check)
	 * @param hkmer    Accumulator to report back the number of reads filtered
	 *                 because of too much reads
	 * @param lkmer    Accumulator to report back the number of reads filtered
	 *                 because of too few reads
	 * @return Whether this kmer should be filtered or not
	 */
	public static boolean filterKmers(Tuple2<KmerIgnoreData, Iterable<Tuple2<Long, Integer>>> kmer, int stackMin,
			int stackMax, LongAccumulator hkmer, LongAccumulator lkmer) {

		int count = 0;
		Iterator<Tuple2<Long, Integer>> it = kmer._2.iterator();

		while (it.hasNext()) {
			count++;
			it.next();
		}
		if ((stackMax != -1 && count > stackMax) || (stackMin != -1 && count < stackMin)) {
			if ((stackMax != -1) && (count > stackMax)) {
				hkmer.add(1);
			}
			if ((stackMin != -1) && (count < stackMin)) {
				lkmer.add(1);
			}

			return true;
		}

		return false;
	}

	/**
	 * Generates a list of tuples with IGN data for each node being referenced by a
	 * kmer.
	 * 
	 * @param kmer The kmer ignore data, with a list of the nodes referencing it
	 * @return The list of tuples with the ID of the nodes, the type of the IGN
	 *         (true for ignP, false for ignS) and the position where the kmer was
	 *         found in the node
	 */
	public static Iterable<Tuple2<Long, Tuple2<Boolean, Integer>>> generateNodesIgn(
			Tuple2<KmerIgnoreData, Iterable<Tuple2<Long, Integer>>> kmer) {

		List<Tuple2<Long, Tuple2<Boolean, Integer>>> out = new LinkedList<>();
		Iterator<Tuple2<Long, Integer>> it = kmer._2.iterator();
		KmerIgnoreData key = kmer._1;
		while (it.hasNext()) {
			Tuple2<Long, Integer> e = it.next();

			Integer pos = e._2;

			out.add(new Tuple2<Long, Tuple2<Boolean, Integer>>(e._1,
					new Tuple2<Boolean, Integer>(key.isIgnP(), pos)));
		}

		return out;

	}

	/**
	 * Computes a single split for this SubPhase.
	 * @param nodeData The input data
	 * @param splitIndex The split to compute
	 * @param maxSplits The total number of splits
	 * @param splitPrime The prime to use for split distribution
	 * @param splitStrategy The split strategy being used
	 * @return The ignore data for the nodes of this split
	 */
	private JavaPairRDD<Long, Iterable<Tuple2<Boolean, Integer>>> computeSplit(JavaPairRDD<Long, Node> nodeData,
			int splitIndex, int maxSplits, int splitPrime, IPhaseSplitStrategy splitStrategy) {

		JavaPairRDD<KmerIgnoreData, Tuple2<Long, Integer>> kmersToIgn;
		kmersToIgn = nodeData
				.flatMapToPair(line -> emitKmers(line, k, splitIndex, maxSplits, splitPrime, splitStrategy, filter_P, filter_S).iterator());

		JavaPairRDD<KmerIgnoreData, Iterable<Tuple2<Long, Integer>>> kmersToIgnGrouped;
		kmersToIgnGrouped = kmersToIgn.groupByKey(new HashPartitioner(nodeData.getNumPartitions()));

		JavaPairRDD<KmerIgnoreData, Iterable<Tuple2<Long, Integer>>> kmersToIgnFiltered;
		kmersToIgnFiltered = kmersToIgnGrouped.filter(data -> filterKmers(data, stackMin, stackMax, hkmer, lkmer));

		JavaPairRDD<Long, Tuple2<Boolean, Integer>> kmersListTmp;
		kmersListTmp = kmersToIgnFiltered.flatMapToPair(data -> generateNodesIgn(data).iterator());

		JavaPairRDD<Long, Iterable<Tuple2<Boolean, Integer>>> kmersList;
		kmersList = kmersListTmp.groupByKey(new HashPartitioner(nodeData.getNumPartitions()));

		return kmersList;
	}

	/**
	 * Runs this SubPhase.
	 * @param nodeData The input dataset
	 * @param splitStrategy The split strategy currently being used
	 * @return The ignore data for each node
	 */
	public JavaPairRDD<Long, KmerCount> run(JavaPairRDD<Long, Node> nodeData, IPhaseSplitStrategy splitStrategy) {

		int kmerSplits = splitStrategy.getSplits(SPLIT_MULTIPLIER);
		logger.info("Computing LargeKmerFilter using " + kmerSplits + " splits");
		
		int splitPrime = splitStrategy.computeSplitPrime(kmerSplits, nodeData.getNumPartitions());
		
		JavaPairRDD<Long, Iterable<Tuple2<Boolean, Integer>>> kmersCounted;

		kmersCounted = this.computeSplit(nodeData, 0, kmerSplits, splitPrime, splitStrategy);

		for (int i = 1; i < kmerSplits; i++) {
			kmersCounted.count();
			JavaPairRDD<Long, Iterable<Tuple2<Boolean, Integer>>> aux;
			aux = this.computeSplit(nodeData, i, kmerSplits, splitPrime, splitStrategy);

			kmersCounted = kmersCounted.fullOuterJoin(aux, new HashPartitioner(nodeData.getNumPartitions())).mapToPair(e -> {
				if (e._2._1.isPresent()) {
					if (e._2._2.isPresent()) {
						List<Tuple2<Boolean, Integer>> outList = new LinkedList<>();

						Iterator<Tuple2<Boolean, Integer>> it;
						it = e._2._1.get().iterator();
						while (it.hasNext()) {
							outList.add(it.next());
						}
						it = e._2._2.get().iterator();
						while (it.hasNext()) {
							outList.add(it.next());
						}
						return new Tuple2<>(e._1, outList);
					} else {
						return new Tuple2<>(e._1, e._2._1.get());
					}
				} else {
					return new Tuple2<>(e._1, e._2._2.get());
				}
			});
			aux.unpersist(false);

		}

		/**
		 * Then, we join that data with the original Node Data
		 */
		JavaPairRDD<Long, Tuple2<Node, Iterable<Tuple2<Boolean, Integer>>>> dataJoined;
		dataJoined = nodeData.join(kmersCounted, new HashPartitioner(nodeData.getNumPartitions()));

		/**
		 * Finally, we format the output to a more readable format.
		 */
		JavaPairRDD<Long, KmerCount> dataFormatted;
		dataFormatted = dataJoined.mapToPair(e -> {
			KmerCount out = new KmerCount();

			out.node = e._2._1;

			Iterator<Tuple2<Boolean, Integer>> it;
			it = e._2._2.iterator();
			while (it.hasNext()) {
				Tuple2<Boolean, Integer> element = it.next();

				if (Boolean.TRUE.equals(element._1)) {
					out.count_P.add(element._2);
				} else {
					out.count_F.add(element._2);
				}
			}

			return new Tuple2<>(e._1, out);
		});

		return dataFormatted;

	}

	/**
	 * Default constructor of CountKmers.
	 * @param k The k being used at this execution
	 * @param filter_P Whether the PinchCorrect filter should be run
	 * @param filter_S Whether the SpreadCorrect filter should be run
	 * @param jsc The JavaSparkContext
	 * @param stackMax The maximum number of kmer reads
	 * @param stackMin The minimum number of kmer reads
	 */
	public CountKmers(int k, boolean filter_P, boolean filter_S, JavaSparkContext jsc, int stackMax,
			int stackMin) {
		this.k = k;
		this.filter_P = filter_P;
		this.filter_S = filter_S;
		this.stackMax = stackMax;
		this.stackMin = stackMin;
		this.hkmer = jsc.sc().longAccumulator();
		this.lkmer = jsc.sc().longAccumulator();
	}

}

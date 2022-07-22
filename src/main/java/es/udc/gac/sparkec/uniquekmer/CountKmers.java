package es.udc.gac.sparkec.uniquekmer;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.sequence.IDNASequence;
import es.udc.gac.sparkec.split.IPhaseSplitStrategy;
import scala.Tuple2;

/**
 * SubPhase of UniqueKmerFilter whose goal is to count the number of kmers that
 * are found only once(twice since it could be found reversed) per each
 * node.
 */
@SuppressWarnings("serial")
public class CountKmers implements Serializable {

	private static final Logger logger = LogManager.getLogger();
	
	private static final int SPLIT_MULTIPLIER = 1;
	
	private static final short UNIQUE_READ = 2;

	
	private final int k;

	/**
	 * Default constructor for the CountKmers SubPhase
	 * @param k The k being used by this execution
	 * @param jsc The JavaSparkContext
	 */
	public CountKmers(int k, JavaSparkContext jsc) {
		this.k = k;
	}

	/**
	 * Generates k-mers for each node, reversing them to choose always the minor in
	 * alphabetic order. Also, it only generates k-mers who are not over any 'N'
	 * 
	 * @param element    Tuple containing the node and it's id
	 * @param split      The current split being computed (ranging from 0 to
	 *                   splitCount)
	 * @param splitCount The number of splits to compute
	 * @param splitPrime Prime number to distribute the splits
	 * @param splitStrategy The split strategy currently being used
	 * @param k The k being used at this execution
	 * @return An iterator of tuples with the kmer and the ID of the node who
	 *         generated it
	 */
	public static Iterable<Tuple2<IDNASequence, Long>> emmitKmers(Tuple2<Long, Node> element, int split, int splitCount,
			int splitPrime, IPhaseSplitStrategy splitStrategy, int k) {
		List<Tuple2<IDNASequence, Long>> out = new LinkedList<>();
		Node node = element._2;

		int end = node.getSeq().length() - k + 1;

		for (int i = 0; i < end; i++) {

			IDNASequence window_tmp = node.getSeq().getSubSequence(i, i + k - 1);

			boolean invalidKmer = window_tmp.containsBase((byte) 'N');
			if (invalidKmer) {
				continue;
			}

			IDNASequence window_tmp_r = window_tmp.rcSequence();

			boolean reversed = window_tmp.compareTo(window_tmp_r) > 0;

			if (!reversed) {
				if (split != splitStrategy.computeItemSplitIndex(window_tmp, splitCount, splitPrime)) {
					continue;
				}
				out.add(new Tuple2<IDNASequence, Long>(window_tmp, element._1));
			} else {
				if (split != splitStrategy.computeItemSplitIndex(window_tmp_r, splitCount, splitPrime)) {
					continue;
				}
				out.add(new Tuple2<IDNASequence, Long>(window_tmp_r, element._1));
			}
		}

		return out;
	}

	/**
	 * Filters out the kmers that are referenced by more than one node (in fact, two
	 * given that the kmer may be found reversed)
	 * 
	 * @param kmers The kmer to check
	 * @return Whether this kmer is reference by more than two nodes
	 */
	public static boolean filterKmers(Tuple2<IDNASequence, Iterable<Long>> kmers) {
		Iterator<Long> it = kmers._2.iterator();

		int i = 0;
		while (it.hasNext()) {
			i++;
			if (i > UNIQUE_READ) {
				return false;
			}
			it.next();
		}

		return true;

	}

	/**
	 * Emits a list of the nodes referenced by a kmer
	 * 
	 * @param kmers The tuple with the kmer and the node referenced
	 * @return The node ID and 1
	 */
	public static Iterable<Tuple2<Long, Integer>> countNodes(Tuple2<IDNASequence, Iterable<Long>> kmers) {
		List<Tuple2<Long, Integer>> out = new LinkedList<>();
		Iterator<Long> it = kmers._2.iterator();

		while (it.hasNext()) {
			Long tmp = it.next();
			out.add(new Tuple2<Long, Integer>(tmp, 1));
		}

		return out;
	}

	/**
	 * Computes a single split for this SubPhase.
	 * @param nodes The input nodes DataSet
	 * @param split The current split being run
	 * @param splitCount The number of splits being run
	 * @param splitPrime The prime number needed to compute the split assigned to each kmer
	 * @param splitStrategy The split strategy being used
	 * @return The counted nodes for this split
	 */
	private JavaPairRDD<Long, Integer> computeSplit(JavaPairRDD<Long, Node> nodes, int split, int splitCount,
			int splitPrime, IPhaseSplitStrategy splitStrategy) {

		JavaPairRDD<IDNASequence, Long> kmers;
		kmers = nodes.flatMapToPair(data -> emmitKmers(data, split, splitCount, splitPrime, splitStrategy, k).iterator());

		JavaPairRDD<IDNASequence, Iterable<Long>> kmersGrouped;
		kmersGrouped = kmers.groupByKey(new HashPartitioner(nodes.getNumPartitions()));

		JavaPairRDD<IDNASequence, Iterable<Long>> kmersFiltered;
		kmersFiltered = kmersGrouped.filter(CountKmers::filterKmers);

		JavaPairRDD<Long, Integer> nodesCounted;
		nodesCounted = kmersFiltered.flatMapToPair(data -> countNodes(data).iterator());

		JavaPairRDD<Long, Integer> nodesCountedFormatted;
		nodesCountedFormatted = nodesCounted.reduceByKey(new HashPartitioner(nodes.getNumPartitions()),
				(Integer a, Integer b) -> a + b);

		return nodesCountedFormatted;

	}

	/**
	 * Runs this subphase.
	 * @param in The input dataset
	 * @param splitStrategy The split strategy being used
	 * @return The counted nodes
	 */
	public JavaPairRDD<Long, Integer> run(JavaPairRDD<Long, Node> in, IPhaseSplitStrategy splitStrategy) {

		int splitCount = splitStrategy.getSplits(SPLIT_MULTIPLIER);
		
		logger.info("Computing UniqueKmerFilter using " + splitCount + " splits");
		
		
		int splitPrime = splitStrategy.computeSplitPrime(splitCount, in.getNumPartitions());

		JavaPairRDD<Long, Integer> result = this.computeSplit(in, 0, splitCount, splitPrime, splitStrategy);

		for (int i = 1; i < splitCount; i++) {
			result.count();
			JavaPairRDD<Long, Integer> tmpResult = this.computeSplit(in, i, splitCount, splitPrime, splitStrategy);
			JavaPairRDD<Long, Tuple2<Optional<Integer>, Optional<Integer>>> tmpJoined;

			tmpJoined = tmpResult.fullOuterJoin(result, new HashPartitioner(in.getNumPartitions()));

			result = tmpJoined.mapToPair(e -> {
				int v = 0;
				v += e._2._1.isPresent() ? e._2._1.get() : 0;
				v += e._2._2.isPresent() ? e._2._2.get() : 0;

				return new Tuple2<>(e._1, v);
			});

		}

		return result;
	}
}

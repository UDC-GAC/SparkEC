package es.udc.gac.sparkec.test.pinchcorrect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.runner.RunWith;

import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.pinchcorrect.PinchCorrectRecommendation;
import es.udc.gac.sparkec.pinchcorrect.ReadInfo;
import es.udc.gac.sparkec.sequence.EagerDNASequence;
import es.udc.gac.sparkec.sequence.IDNASequence;
import es.udc.gac.sparkec.test.utils.NodeGenerator;
import es.udc.gac.sparkec.test.utils.QuickCheckTest;
import scala.Tuple2;

@RunWith(JUnitQuickcheck.class)
public class PinchCorrectRecommendationQuickCheck extends QuickCheckTest {

	@Property
	public void testGenerateKmers(Long nodeId, @From(NodeGenerator.class) Node node) {
		Tuple2<Long, Node> input = new Tuple2<>(nodeId, node);

		Iterable<Tuple2<IDNASequence, ReadInfo>> kmers = PinchCorrectRecommendation.generateKmers(input, 0, 1, 24, null,
				null, 1, this.getSplitStrategy());

		Iterator<Tuple2<IDNASequence, ReadInfo>> it = kmers.iterator();

		String qv = new String(node.getQv());

		while (it.hasNext()) {
			Tuple2<IDNASequence, ReadInfo> kmerData = it.next();

			assertEquals(kmerData._2.getId(), nodeId.longValue());

			String seq = node.getSeq().toString();

			String kmer = kmerData._1.toString();
			String kmer_left = kmer.substring(0, 12);
			String kmer_right = kmer.substring(12, 24);
			char base = kmerData._2.getBase();
			if (!kmerData._2.isDir()) {
				// Reversed
				kmer = new EagerDNASequence((kmer_left + kmer_right).getBytes()).rcSequence(true).toString();
				kmer_left = kmer.substring(0, 12);
				kmer_right = kmer.substring(12, 23);
				base = (char)IDNASequence.getAlternateBase((byte)base);
			}

			assertTrue(seq.contains(kmer_left));
			assertTrue(seq.contains(kmer_right));
			assertFalse(seq.contains(kmer));

			assertEquals(base, seq.charAt(kmerData._2.getPos()));
			assertEquals(kmerData._2.getQv(), qv.charAt(kmerData._2.getPos()));

			assertFalse(kmer.contains("N"));

		}
	}

}

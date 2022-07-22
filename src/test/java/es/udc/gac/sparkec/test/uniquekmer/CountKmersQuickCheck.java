package es.udc.gac.sparkec.test.uniquekmer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.runner.RunWith;

import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.sequence.IDNASequence;
import es.udc.gac.sparkec.test.utils.NodeGenerator;
import es.udc.gac.sparkec.test.utils.QuickCheckTest;
import es.udc.gac.sparkec.uniquekmer.CountKmers;
import scala.Tuple2;

@RunWith(JUnitQuickcheck.class)
public class CountKmersQuickCheck extends QuickCheckTest {

	@Property
	public void kmersGeneration(Long nodeId,@From(NodeGenerator.class) Node node) {
		Tuple2<Long, Node> input = new Tuple2<>(nodeId, node);

		IDNASequence seq = input._2.getSeq();

		Iterable<Tuple2<IDNASequence, Long>> result = CountKmers.emmitKmers(input, 0, 1, 1, this.getSplitStrategy(), 24);

		Iterator<Tuple2<IDNASequence, Long>> it = result.iterator();
		while (it.hasNext()) {
			Tuple2<IDNASequence, Long> kmer = it.next();

			assertEquals(kmer._2, input._1);
			assertTrue(seq.toString().contains(kmer._1.toString()) || seq.toString().contains(kmer._1.rcSequence().toString()));

			assertFalse(kmer._1.toString().contains("N"));
			assertEquals(24, kmer._1.length());
		}

	}

}

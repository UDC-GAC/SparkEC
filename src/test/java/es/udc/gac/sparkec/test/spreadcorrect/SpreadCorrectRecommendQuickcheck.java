package es.udc.gac.sparkec.test.spreadcorrect;

import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.runner.RunWith;

import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.sequence.IDNASequence;
import es.udc.gac.sparkec.spreadcorrect.SpreadCorrectKmerReadData;
import es.udc.gac.sparkec.spreadcorrect.SpreadCorrectRecommend;
import es.udc.gac.sparkec.test.utils.NodeGeneratorWithIgn;
import es.udc.gac.sparkec.test.utils.QuickCheckTest;
import scala.Tuple2;

@RunWith(JUnitQuickcheck.class)
public class SpreadCorrectRecommendQuickcheck extends QuickCheckTest {

	@Property
	public void testGenerateKmers(long nodeId, @From(NodeGeneratorWithIgn.class) Node node) {

		Iterable<Tuple2<IDNASequence, SpreadCorrectKmerReadData>> output = SpreadCorrectRecommend
				.generateKmers(new Tuple2<Long, Node>(nodeId, node), 24, -1, null, 0, 0, 1, 1, this.getSplitStrategy());

		Iterator<Tuple2<IDNASequence, SpreadCorrectKmerReadData>> it = output.iterator();

		while (it.hasNext()) {
			Tuple2<IDNASequence, SpreadCorrectKmerReadData> kmer = it.next();

			assertTrue(node.getSeq().toString().substring(kmer._2.getPos()).startsWith(kmer._1.toString())
					|| node.getSeq().toString().contains(kmer._1.rcSequence().toString()));

		}
	}

}

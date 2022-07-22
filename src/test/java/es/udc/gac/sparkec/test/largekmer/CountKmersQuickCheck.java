package es.udc.gac.sparkec.test.largekmer;

import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.runner.RunWith;

import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;

import es.udc.gac.sparkec.largekmer.CountKmers;
import es.udc.gac.sparkec.largekmer.KmerIgnoreData;
import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.test.utils.NodeGenerator;
import es.udc.gac.sparkec.test.utils.QuickCheckTest;
import scala.Tuple2;

@RunWith(JUnitQuickcheck.class)
public class CountKmersQuickCheck extends QuickCheckTest {

	
	@Property
	public void testEmitKmers(Long id, @From(NodeGenerator.class) Node node) {
		Tuple2<Long, Node> input = new Tuple2<>(id, node);
		
		Iterable<Tuple2<KmerIgnoreData, Tuple2<Long, Integer>>> output = CountKmers.emitKmers(input, 24, 0, 1, 1, getSplitStrategy(), true, true);
		
		Iterator<Tuple2<KmerIgnoreData, Tuple2<Long, Integer>>> it = output.iterator();
		
		while(it.hasNext()) {
			Tuple2<KmerIgnoreData, Tuple2<Long, Integer>> kmer = it.next();
			
			if (kmer._1.isIgnP()) {
				
				assertTrue(kmer._1.getKmer().length() == 24);
				assertTrue(
					(node.getSeq().getValue(kmer._2._2 + 11) == kmer._1.getKmer().getValue(11)) || 
					(node.getSeq().getValue(kmer._2._2 + 11) == kmer._1.getKmer().rcSequence().getValue(11))
				);
				assertTrue(
					(node.getSeq().getValue(kmer._2._2 + 13) == kmer._1.getKmer().getValue(12)) ||
					(node.getSeq().getValue(kmer._2._2 + 13) == kmer._1.getKmer().rcSequence().getValue(12))
				);
			} else {
				assertTrue(
					node.getSeq().toString().contains(kmer._1.getKmer().toString()) || 
					node.getSeq().toString().contains(kmer._1.getKmer().rcSequence().toString())
				);
			}
			
			
			assertTrue(kmer._2._1.equals(id));
		}
	}

}

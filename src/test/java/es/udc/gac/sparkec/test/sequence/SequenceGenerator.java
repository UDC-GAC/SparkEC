package es.udc.gac.sparkec.test.sequence;

import java.util.Arrays;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

import es.udc.gac.sparkec.sequence.EagerDNASequence;
import es.udc.gac.sparkec.sequence.IDNASequence;
import es.udc.gac.sparkec.sequence.LazyDNASequence;

public class SequenceGenerator extends Generator<IDNASequence> {

	private static final Byte[] SEQ_BASES = {
		'A',
		'T',
		'C',
		'G'
	};

	
	@SuppressWarnings("unchecked")
	public SequenceGenerator() {
		super(Arrays.asList(new Class[] {LazyDNASequence.class, EagerDNASequence.class}));
	}
	
	
	private static byte[] generateSeq(SourceOfRandomness random, int size) {
		byte[] buffer = new byte[size];
		
		for (int i = 0; i < size; i++) {
			if (random.nextInt(0, 1000) < 2) {
				buffer[i] = 'N';
			} else {
				buffer[i] = random.choose(SEQ_BASES);
			}
		}
		return buffer;
	}

	@Override
	public IDNASequence generate(SourceOfRandomness random, GenerationStatus status) {
		if(random.nextBoolean()) {
			return new LazyDNASequence(generateSeq(random, random.nextInt(10, 100)));
		} else {
			return new EagerDNASequence(generateSeq(random, random.nextInt(10, 100)));
		}
		
		
	}

}

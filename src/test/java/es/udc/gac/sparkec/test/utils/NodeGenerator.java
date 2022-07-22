package es.udc.gac.sparkec.test.utils;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.sequence.LazyDNASequence;

public class NodeGenerator extends Generator<Node> {

	private static final Byte[] SEQ_BASES = { 'A', 'T', 'C', 'G' };

	private static final byte MIN_QV = (byte) 33;
	private static final byte MAX_QV = (byte) 126;

	public NodeGenerator() {
		super(Node.class);
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

	private static byte[] generateQv(SourceOfRandomness random, int size) {
		byte[] buffer = new byte[size];

		for (int i = 0; i < size; i++) {
			buffer[i] = random.nextByte(MIN_QV, MAX_QV);
		}

		return buffer;
	}

	public static Node generate(SourceOfRandomness random) {

		int size = random.nextInt(50, 200);

		byte[] seq = generateSeq(random, size);
		byte[] qv = generateQv(random, size);

		return new Node(new LazyDNASequence(seq), qv, 1.0f);
	}

	@Override
	public Node generate(SourceOfRandomness random, GenerationStatus status) {

		int size = random.nextInt(50, 200);

		byte[] seq = generateSeq(random, size);
		byte[] qv = generateQv(random, size);

		return new Node(new LazyDNASequence(seq), qv, 1.0f);
	}

}

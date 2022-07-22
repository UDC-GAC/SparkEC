package es.udc.gac.sparkec.test.utils;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

import es.udc.gac.sparkec.node.EncodingUtils;
import es.udc.gac.sparkec.node.Node;

public class NodeGeneratorWithIgn extends Generator<Node> {
	
	public NodeGeneratorWithIgn() {
		super(Node.class);
	}
	
	private static String generateIgn(SourceOfRandomness random, int length) {
		byte[] buffer = new byte[(int) (Math.ceil(length / 4.0) * 4)];
		
		for (int i = 0; i < length; i++) {
			if (random.nextInt(0,1000) < 1) {
				buffer[i] = 1;
			}
		}
		
		
		return new String(EncodingUtils.str2hex(buffer));
	}

	@Override
	public Node generate(SourceOfRandomness random, GenerationStatus status) {
		Node node = NodeGenerator.generate(random);
		
		node.setIgnF(generateIgn(random, node.getSeq().length()));
		node.setIgnP(generateIgn(random, node.getSeq().length()));
		
		return node;
	}

}

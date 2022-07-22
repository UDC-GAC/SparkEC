package es.udc.gac.sparkec.test.node;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import es.udc.gac.sparkec.node.InvalidNodeFormatException;
import es.udc.gac.sparkec.node.Node;
import es.udc.gac.sparkec.sequence.EagerDNASequence;

public class NodeTest {

	@Test
	public void testCloudECString() throws InvalidNodeFormatException {

		Node node = new Node();
		node.setSeq(new EagerDNASequence("ACGATCAGAT".getBytes()));
		node.setQv("FTHRDVRHER".getBytes());
		node.setCoverage(1.0f);
		node.setIgnF("FFFFFF");
		node.setIgnP("FFFFFF");

		assertEquals(Node.fromCloudECString(node.toCloudECString("A"))._2.toString(), node.toString());
	}

	@Test
	public void testSFQString() throws InvalidNodeFormatException {
		Node node = new Node();
		node.setSeq(new EagerDNASequence("ACGATCAGAT".getBytes()));
		node.setQv("FTHRDVRHER".getBytes());

		assertEquals(Node.fromSFQString(node.toSFQString("A"))._2.toString(), node.toString());
	}

}

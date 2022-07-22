package es.udc.gac.sparkec.test.sequence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Random;

import org.junit.runner.RunWith;

import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;

import es.udc.gac.sparkec.sequence.IDNASequence;
import es.udc.gac.sparkec.sequence.LazyDNASequence;
import es.udc.gac.sparkec.sequence.UnableToSkipMiddleBase;

@RunWith(JUnitQuickcheck.class)
public class SequenceQuickCheck {

	@Property
	public void testSharingDataReference(@From(SequenceGenerator.class) IDNASequence sequence) throws NoSuchFieldException,
			SecurityException, IllegalAccessException, UnableToSkipMiddleBase {

		Field dataField = LazyDNASequence.class.getDeclaredField("data");
		dataField.setAccessible(true);
		
		LazyDNASequence reversedSequence;
		try {
			reversedSequence = (LazyDNASequence) sequence.reverseSequence(false);
		} catch(ClassCastException e) {
			return;
		}
		

		assertEquals(System.identityHashCode(dataField.get(sequence)),
				System.identityHashCode(dataField.get(reversedSequence)));

		LazyDNASequence alternateSequence = (LazyDNASequence) sequence.invertSequence(false);

		assertEquals(System.identityHashCode(dataField.get(sequence)),
				System.identityHashCode(dataField.get(alternateSequence)));

		LazyDNASequence subsequence = (LazyDNASequence) sequence.getSubSequence(0, sequence.length() - 2, false);

		assertEquals(System.identityHashCode(dataField.get(sequence)),
				System.identityHashCode(dataField.get(subsequence)));

		LazyDNASequence subsequence_k1 = (LazyDNASequence) sequence.getSubSequenceWithoutMiddleBase(0, sequence.length() - 3, false);

		assertEquals(System.identityHashCode(dataField.get(sequence)),
				System.identityHashCode(dataField.get(subsequence_k1)));

		LazyDNASequence multipleOperations = (LazyDNASequence) reversedSequence.getSubSequence(0, sequence.length() - 2, false);

		assertEquals(System.identityHashCode(dataField.get(sequence)),
				System.identityHashCode(dataField.get(multipleOperations)));

	}

	@Property
	public void testDoubleReverse(@From(SequenceGenerator.class) IDNASequence sequence) {
		assertEquals(sequence, sequence.reverseSequence(false).reverseSequence(false));
	}

	@Property
	public void testDoubleInvert(@From(SequenceGenerator.class) IDNASequence sequence) {
		assertEquals(sequence, sequence.invertSequence(false).invertSequence(false));
	}

	@Property
	public void testDoubleRC(@From(SequenceGenerator.class) IDNASequence sequence) {
		assertEquals(sequence, sequence.rcSequence(false).rcSequence(false));
	}

	@Property
	public void testRCEqualsInvertAndReverse(@From(SequenceGenerator.class) IDNASequence sequence) {
		assertEquals(sequence.rcSequence(false), sequence.invertSequence(false).reverseSequence(false));
	}

	@Property
	public void testReverse(@From(SequenceGenerator.class) IDNASequence sequence) {
		byte[] bufferOri = sequence.toByteArray();
		byte[] bufferReversed = sequence.reverseSequence(false).toByteArray();

		assertEquals(bufferOri.length, bufferReversed.length);

		for (int i = 0; i < bufferOri.length; i++) {
			assertEquals(bufferOri[i], bufferReversed[bufferReversed.length - i - 1]);
		}
	}

	@Property
	public void testInvert(@From(SequenceGenerator.class) IDNASequence sequence) {
		byte[] bufferOri = sequence.toByteArray();
		byte[] bufferInverted = sequence.invertSequence(false).toByteArray();

		assertEquals(bufferOri.length, bufferInverted.length);

		for (int i = 0; i < bufferOri.length; i++) {
			assertEquals(bufferOri[i], IDNASequence.getAlternateBase(bufferInverted[i]));
		}
	}

	@Property
	public void testCommutativeInvertReverse(@From(SequenceGenerator.class) IDNASequence sequence) {
		assertEquals(sequence.invertSequence(false).reverseSequence(false),
				sequence.reverseSequence(false).invertSequence(false));
	}

	@Property
	public void testSharingObjectReference(@From(SequenceGenerator.class) IDNASequence sequence)
			throws UnableToSkipMiddleBase {

		IDNASequence copy = sequence.copy();

		assertNotEquals(System.identityHashCode(copy), System.identityHashCode(copy.reverseSequence(false)));
		assertEquals(System.identityHashCode(copy), System.identityHashCode(copy.reverseSequence(true)));

		copy = sequence.copy();

		assertNotEquals(System.identityHashCode(copy), System.identityHashCode(copy.invertSequence(false)));
		assertEquals(System.identityHashCode(copy), System.identityHashCode(copy.invertSequence(true)));

		copy = sequence.copy();

		assertNotEquals(System.identityHashCode(copy), System.identityHashCode(copy.rcSequence(false)));
		assertEquals(System.identityHashCode(copy), System.identityHashCode(copy.rcSequence(true)));

		copy = sequence.copy();

		assertNotEquals(System.identityHashCode(copy), System.identityHashCode(copy.getSubSequence(0, 8, false)));
		assertEquals(System.identityHashCode(copy), System.identityHashCode(copy.getSubSequence(0, 8, true)));

		copy = sequence.copy();

		assertNotEquals(System.identityHashCode(copy),
				System.identityHashCode(copy.getSubSequenceWithoutMiddleBase(0, 5, false)));
		assertEquals(System.identityHashCode(copy),
				System.identityHashCode(copy.getSubSequenceWithoutMiddleBase(0, 5, true)));

	}

	@Property
	public void testSubSequence(@From(SequenceGenerator.class) IDNASequence sequence) {

		IDNASequence subsequence = sequence.getSubSequence(1, 8);

		assertEquals(8, subsequence.length());

		for (int i = 0; i < subsequence.length(); i++) {
			assertEquals(subsequence.getValue(i), sequence.getValue(i + 1));
		}

	}

	@Property
	public void testSubSequenceK1(@From(SequenceGenerator.class) IDNASequence sequence)
			throws UnableToSkipMiddleBase {

		IDNASequence subsequence = sequence.getSubSequenceWithoutMiddleBase(1, 8);

		assertEquals(7, subsequence.length());

		for (int i = 0; i < subsequence.length(); i++) {
			if (i < subsequence.length() / 2) {
				assertEquals(subsequence.getValue(i), sequence.getValue(i + 1));
			} else {
				assertEquals(subsequence.getValue(i), sequence.getValue(i + 2));
			}

		}

	}

	@Property
	public void testCOW(@From(SequenceGenerator.class) IDNASequence sequence) {
		LazyDNASequence reversed;
		try {
			reversed = (LazyDNASequence) sequence.reverseSequence(false);
		} catch(ClassCastException e) {
			return;
		}

		reversed.setValue(0, (byte) 'X');

		assertNotEquals(reversed.reverseSequence(false), sequence);

		reversed = (LazyDNASequence) sequence.reverseSequence(false);

		sequence.setValue(0, (byte) 'X');

		assertNotEquals(reversed.reverseSequence(false), sequence);
	}

	@Property
	public void testSerialization(@From(SequenceGenerator.class) IDNASequence sequence)
			throws IOException, ClassNotFoundException {

		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		new ObjectOutputStream(stream).writeObject(sequence);
		byte[] bytes = stream.toByteArray();

		IDNASequence result = (IDNASequence) new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();

		assertEquals(result, sequence);
	}

	@Property
	public void testAppend(@From(SequenceGenerator.class) IDNASequence a, @From(SequenceGenerator.class) IDNASequence b,
			@From(SequenceGenerator.class) IDNASequence c) {

		assertEquals(a.toString() + b.toString(), a.append(b).toString());
		
		assertEquals(a.toString() + b.toString() + c.toString(), a.append(b,c).toString());
	}
	
	@Property
	public void testContains(@From(SequenceGenerator.class) IDNASequence sequence) {
		Random generator = new Random();
		
		for (int i = 0; i < 100; i++) {
			assertTrue(sequence.containsBase(sequence.getValue(generator.nextInt(sequence.length()))));
		}
		
		assertFalse(sequence.containsBase((byte)'X'));
		
	}

}

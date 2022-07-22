package es.udc.gac.sparkec.sequence;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * This is the base implementation for a DNA Sequence. It uses a byte array to store all the bases of
 * the Sequence. when a transformation is used over this Sequence, a computation is made over the entire
 * Sequence, and a new array allocation takes place.
 */
public class EagerDNASequence implements IDNASequence {

	private static final long serialVersionUID = 1L;

	private byte[] data;

	/**
	 * Internal method to get, either self reference, or a new Sequence reference,
	 * depending on a flag.
	 * 
	 * @param reuseSequence Whether self reference should be returned
	 * @return The reference of the Sequence
	 */
	private EagerDNASequence getSequenceReference(boolean reuseSequence) {
		if (reuseSequence) {
			return this;
		} else {
			return new EagerDNASequence(this.data);
		}
	}

	/**
	 * Iterator for EagerDNASequence.
	 */
	private class EagerDNASequenceIterator implements Iterator<Byte> {

		/**
		 * The DNA Sequence being iterated.
		 */
		private EagerDNASequence reference;
		
		/**
		 * The current position of this iterator.
		 */
		private int pos;

		/**
		 * Default constructor for the EagerDNASequenceIterator.
		 * @param reference The reference to the EagerDnaSequence
		 */
		public EagerDNASequenceIterator(EagerDNASequence reference) {
			this.reference = reference;
			this.pos = 0;
		}

		@Override
		public boolean hasNext() {
			return pos < reference.length();
		}

		@Override
		public Byte next() {
			if (!this.hasNext()) {
				throw new NoSuchElementException();
			}
			return reference.getValue(pos++);
		}

	}

	@Override
	public Iterator<Byte> iterator() {
		return new EagerDNASequenceIterator(this);
	}

	@Override
	public byte[] toByteArray() {
		return Arrays.copyOf(data, data.length);
	}

	@Override
	public byte[] getRawData() {
		return data;
	}

	@Override
	public byte getValue(int pos) {
		return data[pos];
	}

	@Override
	public void setValue(int pos, byte value) {
		data[pos] = value;
	}

	@Override
	public int length() {
		return data.length;
	}

	public EagerDNASequence(byte[] data) {
		this.data = Arrays.copyOf(data, data.length);
	}

	@Override
	public IDNASequence getSubSequence(int start, int end, boolean reuseSequence) {

		EagerDNASequence seq = this.getSequenceReference(reuseSequence);

		seq.data = Arrays.copyOfRange(data, start, end + 1);

		return seq;
	}

	@Override
	public IDNASequence reverseSequence(boolean reuseSequence) {

		EagerDNASequence seq = this.getSequenceReference(reuseSequence);

		byte[] newBuf = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			newBuf[data.length - i - 1] = data[i];
		}
		seq.data = newBuf;

		return seq;
	}

	@Override
	public IDNASequence invertSequence(boolean reuseSequence) {

		EagerDNASequence seq = this.getSequenceReference(reuseSequence);

		for (int i = 0; i < seq.data.length; i++) {
			seq.data[i] = IDNASequence.getAlternateBase(seq.data[i]);
		}

		return seq;
	}

	@Override
	public IDNASequence rcSequence(boolean reuseSequence) {

		EagerDNASequence seq = this.getSequenceReference(reuseSequence);

		seq.invertSequence(true);
		seq.reverseSequence(true);

		return seq;

	}

	@Override
	public IDNASequence getSubSequenceWithoutMiddleBase(int start, int end, boolean reuseSequence)
			throws UnableToSkipMiddleBase {

		EagerDNASequence seq = this.getSequenceReference(reuseSequence);

		int middleBase = start + (end - start) / 2;
		int j = 0;
		byte[] buffer = new byte[end - start];   // A T C G A
		for (int i = start; i < end + 1; i++) {
			if (i == middleBase) {
				continue;
			}
			buffer[j] = seq.data[i];
			j++;
		}
		seq.data = buffer;
		return seq;
	}

	@Override
	public boolean containsBase(byte base) {
		for (int i = 0; i < this.length(); i++) {
			if (data[i] == base) {
				return true;
			}
		}
		return false;
	}

	@Override
	public IDNASequence append(boolean reuseSequence, IDNASequence... sequences) {
		EagerDNASequence seq = this.getSequenceReference(reuseSequence);

		int totalLength = 0;
		for (IDNASequence sequence : sequences) {
			totalLength += sequence.length();
		}

		byte[] newData = new byte[this.length() + totalLength];

		System.arraycopy(this.toByteArray(), 0, newData, 0, this.length());

		int currentIndex = this.length();
		for (IDNASequence sequence : sequences) {
			System.arraycopy(sequence.toByteArray(), 0, newData, currentIndex, sequence.length());
			currentIndex += sequence.length();
		}

		seq.data = newData;
		return seq;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(data);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EagerDNASequence other = (EagerDNASequence) obj;
		
		return Arrays.equals(data, other.data);
	}
	
	@Override
	public String toString() {
		return new String(this.data);
	}

	@Override
	public IDNASequence copy() {
		return this.getSequenceReference(false);
	}

}

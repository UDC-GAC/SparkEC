package es.udc.gac.sparkec.sequence;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The LazyDNASequence class allows to efficiently store a DNA sequence in which are
 * expected to happen some common operations, like taking a kmer or reversing
 * the sequence.
 * 
 * <p>
 * This class will share the underlying byte array implementation and,
 * optionally, share its own reference across the multiple operations that can
 * be performed over it. In order to achieve this goal, Copy-on-Write strategies
 * have been implemented on it.
 * 
 * <p>
 * Also, to further improve the performance, all the operations performed to the
 * Sequence class, apart from the setValue method, are implemented via a lazy
 * strategy: no operation will be performed over the array until COW happens,
 * and if a value is queried, in that point all the previous operations queued
 * will be made to that single value.
 * 
 * <p>
 * Moreover, since Java keeps track of the serialized and deserialized id of the
 * objects, the shared references between Sequences should be kept even after the
 * serialization process.
 * 
 */
public class LazyDNASequence implements IDNASequence {

	private static final long serialVersionUID = 1L;

	/**
	 * This class contains static flag values to be used in order to store the
	 * sequence status in a memory friendly way.
	 * 
	 * @author Marco
	 *
	 */
	private static class SequenceStatus implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * The sequence has no special operation applied.
		 */
		public static final byte NORMAL = 0;

		/**
		 * The sequence is reversed
		 */
		public static final byte REVERSED = 1;

		/**
		 * The sequence is using alternative bases.
		 * <ul>
		 * <li>A -> T</li>
		 * <li>T -> A</li>
		 * <li>C -> G</li>
		 * <li>G -> C</li>
		 * </ul>
		 * 
		 */
		public static final byte ALTERNATE_BASES = 2;

		/**
		 * The sequence is skipping its middle base.
		 */
		public static final byte K_1 = 4;

		/**
		 * The sequence is sharing the underlying byte array reference.
		 */
		public static final byte SHARED_REFERENCE = 8;

		private SequenceStatus() {

		}
	}

	/**
	 * The underlying byte array reference
	 */
	private byte[] data;

	/**
	 * The offset where the data of this Sequence starts (relative to the byte
	 * array). If the Sequence is reversed, this offset represents the latest index
	 * that will be used of the underlying data array.
	 */
	private int offset;

	/**
	 * The number of bases this sequence has
	 */
	private int length;

	/**
	 * The transformation status of this sequence
	 */
	private byte status;

	/**
	 * Internal method to get, either self reference, or a new Sequence reference,
	 * depending on a flag.
	 * 
	 * @param reuseSequence Whether self reference should be returned
	 * @return The reference of the Sequence
	 */
	private LazyDNASequence getSequenceReference(boolean reuseSequence) {
		if (reuseSequence) {
			return this;
		} else {
			LazyDNASequence seq = new LazyDNASequence(this.data, this.offset, this.length);
			seq.status = this.status;
			return seq;
		}
	}

	/**
	 * Internal method to compute the final index to access the underlying data,
	 * given a relative position.
	 * 
	 * @param pos The relative position
	 * @return The index to access the data
	 */
	private int computeIndex(int pos) {
		if (((this.status & SequenceStatus.K_1) != 0) && (pos >= (this.length / 2))) {

			if (((this.status & SequenceStatus.REVERSED) != 0) && (pos >= ((this.length + 1) / 2))) {
				pos++;
			}
			if (((this.status & SequenceStatus.REVERSED) == 0) && (pos >= (this.length / 2))) {
				pos++;
			}

		}

		if ((this.status & SequenceStatus.REVERSED) != 0) {
			return this.offset - pos;
		} else {
			return this.offset + pos;
		}

	}

	/**
	 * Transforms this Sequence into a byte array.
	 * 
	 * @return The new byte array
	 */
	public byte[] toByteArray() {
		if ((this.status & SequenceStatus.SHARED_REFERENCE) != 0) {
			byte[] result = new byte[this.length];

			int i = 0;
			while (i < this.length) {
				result[i] = this.getValue(i);
				i++;
			}

			return result;
		} else {
			return this.data;
		}
	}

	/**
	 * Returns a new byte array with the data of this sequence.
	 * A new byte array will get allocated.
	 * 
	 * @return The array with the data
	 */
	public byte[] getRawData() {
		return Arrays.copyOfRange(this.data, this.offset, this.length);
	}

	/**
	 * Returns the DNA base at a given position.
	 * 
	 * @param pos The position
	 * @return The DNA base
	 */
	public byte getValue(int pos) {
		pos = this.computeIndex(pos);

		if ((this.status & SequenceStatus.ALTERNATE_BASES) != 0) {
			return IDNASequence.getAlternateBase(this.data[pos]);
		} else {
			return this.data[pos];
		}
	}

	/**
	 * Sets the base of the Sequence at a given position.Since Copy-on-Write is implemented,
	 *  a new memory allocation could take place using this method.
	 * @param pos   The position
	 * @param value The new base value
	 */
	public void setValue(int pos, byte value) {
		if ((this.status & SequenceStatus.SHARED_REFERENCE) != 0) {
			this.data = this.toByteArray();
			this.offset = 0;
			this.length = data.length;
			this.status = SequenceStatus.NORMAL;
		}
		pos = this.computeIndex(pos);
		this.data[pos] = value;
	}

	/**
	 * Gets the length of this sequence.
	 * 
	 * @return The length of this sequence
	 */
	public int length() {
		return this.length;
	}

	/**
	 * Default constructor for a Sequence using all the data available.
	 * 
	 * @param data The data
	 */
	public LazyDNASequence(byte[] data) {
		super();
		this.data = data;
		this.offset = 0;
		this.length = data.length;
		this.status = SequenceStatus.NORMAL;
	}

	/**
	 * Default constructor for a Sequence using a part of the data available.
	 * 
	 * @param data   The data
	 * @param offset The start position to be used of the data
	 * @param length The number of elements to use
	 */
	public LazyDNASequence(byte[] data, int offset, int length) {
		super();
		this.data = data;
		this.offset = offset;
		this.length = length;
		this.status = SequenceStatus.NORMAL;
	}

	/**
	 * Copy constructor for the Sequence class
	 * 
	 * @param sequence The original Sequence
	 */
	public LazyDNASequence(LazyDNASequence sequence) {
		super();

		sequence.status |= SequenceStatus.SHARED_REFERENCE;

		this.data = sequence.data;
		this.offset = sequence.offset;
		this.length = sequence.length;
		this.status = sequence.status;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(this.toByteArray());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LazyDNASequence other = (LazyDNASequence) obj;
		if (other.length != length) {
			return false;
		}
		
		// TODO: remove iterators here!

		Iterator<Byte> itSelf = this.iterator();
		Iterator<Byte> itOther = other.iterator();

		while (itSelf.hasNext()) {
			if (!itSelf.next().equals(itOther.next())) {
				return false;
			}
		}

		return true;
	}

	@Override
	public String toString() {
		return new String(this.toByteArray());
	}

	/**
	 * Returns a new Sequence with only a part of the bases of this one.
	 * 
	 * @param start         The starting base, inclusive.
	 * @param end           The latest base, inclusive
	 * @param reuseSequence Whether the Sequence object reference should be reused
	 * @return The new Sequence
	 */
	@Override
	public IDNASequence getSubSequence(int start, int end, boolean reuseSequence) {

		int newLength = end - start + 1;
		if (newLength > this.length) {
			throw new IndexOutOfBoundsException("Cannot get a subsequence with a bigger length than the original one!");
		}

		LazyDNASequence sequence = this.getSequenceReference(reuseSequence);

		if ((this.status & SequenceStatus.REVERSED) != 0) {
			sequence.offset -= start;
		} else {
			sequence.offset += start;
		}
		sequence.length = end - start + 1;

		if (!reuseSequence) {
			this.status |= SequenceStatus.SHARED_REFERENCE;
			sequence.status |= SequenceStatus.SHARED_REFERENCE;
		}

		return sequence;
	}

	/**
	 * Returns a new Sequence reversing this one (first base will become latest).
	 * 
	 * @param reuseSequence Whether the Sequence object reference should be reused
	 * @return The new Sequence
	 */
	@Override
	public IDNASequence reverseSequence(boolean reuseSequence) {
		LazyDNASequence sequence = this.getSequenceReference(reuseSequence);

		if ((sequence.status & SequenceStatus.REVERSED) != 0) {
			if ((sequence.status & SequenceStatus.K_1) != 0) {
				sequence.offset -= sequence.length;
			} else {
				sequence.offset -= sequence.length - 1;
			}
		} else {
			if ((sequence.status & SequenceStatus.K_1) != 0) {
				sequence.offset += sequence.length;
			} else {
				sequence.offset += sequence.length - 1;
			}
		}

		sequence.status ^= SequenceStatus.REVERSED; // BitWise XOR
		if (!reuseSequence) {
			this.status |= SequenceStatus.SHARED_REFERENCE;
			sequence.status |= SequenceStatus.SHARED_REFERENCE;
		}

		return sequence;
	}

	/**
	 * Returns a new Sequence with alternate bases of this one.
	 * 
	 * @param reuseSequence Whether the Sequence object reference should be reused
	 * @return The new Sequence
	 */
	@Override
	public IDNASequence invertSequence(boolean reuseSequence) {
		LazyDNASequence sequence = this.getSequenceReference(reuseSequence);

		sequence.status ^= SequenceStatus.ALTERNATE_BASES; // BitWise XOR
		if (!reuseSequence) {
			this.status |= SequenceStatus.SHARED_REFERENCE;
			sequence.status |= SequenceStatus.SHARED_REFERENCE;
		}

		return sequence;
	}

	/**
	 * Computes the RC to this Sequence. A Sequence is an RC of another one when it
	 * is both reversed and using alternate bases.
	 * 
	 * @param reuseSequence Whether the Sequence object reference should be reused
	 * @return The new Sequence
	 */
	@Override
	public IDNASequence rcSequence(boolean reuseSequence) {
		LazyDNASequence sequence = this.getSequenceReference(reuseSequence);

		sequence.invertSequence(true);
		sequence.reverseSequence(true);

		return sequence;
	}

	/**
	 * Computes a new Sequence from a part of this one, without taking it's middle
	 * base. This method will not be available to get called again in the new
	 * Sequence.
	 * 
	 * @param start         The index of the first base of the new Sequence,
	 *                      inclusive
	 * @param end           The index of the latest base of the new Sequence,
	 *                      inclusive
	 * @param reuseSequence Whether the Sequence object reference should be reused
	 * @return The new Sequence
	 * @throws UnableToSkipMiddleBase If this Sequence already skipped it's middle
	 *                                base before
	 */
	@Override
	public IDNASequence getSubSequenceWithoutMiddleBase(int start, int end, boolean reuseSequence)
			throws UnableToSkipMiddleBase {

		int newLength = end - start;
		if (newLength >= this.length) {
			throw new IndexOutOfBoundsException("Cannot get a subsequence with a bigger length than the original one!");
		}

		LazyDNASequence sequence = this.getSequenceReference(reuseSequence);

		if ((sequence.status & (SequenceStatus.K_1 | SequenceStatus.REVERSED)) != 0) {
			throw new UnableToSkipMiddleBase();
		}

		sequence.status |= SequenceStatus.K_1;
		if (!reuseSequence) {
			this.status |= SequenceStatus.SHARED_REFERENCE;
			sequence.status |= SequenceStatus.SHARED_REFERENCE;
		}

		if ((this.status & SequenceStatus.REVERSED) != 0) {
			sequence.offset -= start;
		} else {
			sequence.offset += start;
		}
		sequence.length = end - start;

		return sequence;
	}

	/**
	 * Checks whether this sequence is reversed.
	 * 
	 * @return Whether this sequence is reversed
	 */
	public boolean isReversed() {
		return (this.status & SequenceStatus.REVERSED) != 0;
	}

	/**
	 * Checks whether this sequence is using alternate bases.
	 * 
	 * @return Whether this sequence is using alternate bases
	 */
	public boolean isAlternate() {
		return (this.status & SequenceStatus.ALTERNATE_BASES) != 0;
	}

	/**
	 * Checks whether this sequence is skipping middle base.
	 * 
	 * @return Whether this sequence is skipping middle base.
	 */
	public boolean isSkippingMiddleBase() {
		return (this.status & SequenceStatus.K_1) != 0;
	}

	/**
	 * Checks whether this sequence is RC, meaning it is both reversed and using
	 * alternate bases.
	 * 
	 * @return Whether this sequence is RC
	 */
	public boolean isRCSequence() {
		return this.isAlternate() && this.isReversed();
	}

	/**
	 * Checks whether this sequence is currently still sharing the underlying data
	 * reference.
	 * 
	 * @return Whether this sequence is sharing the data reference
	 */
	public boolean isSharingReference() {
		return (this.status & SequenceStatus.SHARED_REFERENCE) != 0;
	}

	/**
	 * Class implementing the Iterator pattern for a LazyDNASequence.
	 * 
	 */
	public class SequenceIterator implements Iterator<Byte> {

		/**
		 * The current position being iterated.
		 */
		int currentPos;

		/**
		 * Reference to the Sequence being iterated.
		 */
		LazyDNASequence sequence;

		@Override
		public boolean hasNext() {
			return this.currentPos < (this.sequence.length());
		}

		@Override
		public Byte next() {
			if (!this.hasNext()) {
				throw new NoSuchElementException();
			}
			return this.sequence.getValue(this.currentPos++);
		}

		protected SequenceIterator(LazyDNASequence sequence) {
			this.sequence = sequence;
			this.currentPos = 0;
		}

	}

	@Override
	public Iterator<Byte> iterator() {
		return new SequenceIterator(this);
	}

	/**
	 * Checks whether this Sequence contains the given base.
	 * 
	 * @param base The base to look for
	 * @return Whether the Sequence did contain the base
	 */
	@Override
	public boolean containsBase(byte base) {
		int i = 0;
		while (i < this.length) {
			if (this.getValue(i) == base) {
				return true;
			}
			i++;
		}
		return false;
	}

	/**
	 * Appends many sequences to this sequence. 
	 * @param reuseSequence Whether this sequence reference should be reused
	 * @param sequences      The sequences to append to this sequence
	 * @return The resulting sequence
	 */
	@Override
	public IDNASequence append(boolean reuseSequence, IDNASequence... sequences) {
		LazyDNASequence result = this.getSequenceReference(reuseSequence);

		int totalLength = 0;
		for (IDNASequence sequence : sequences) {
			totalLength += sequence.length();
		}

		byte[] newData = new byte[this.length + totalLength];

		System.arraycopy(this.toByteArray(), 0, newData, 0, this.length);

		int currentIndex = this.length;
		for (IDNASequence sequence : sequences) {
			System.arraycopy(sequence.toByteArray(), 0, newData, currentIndex, sequence.length());
			currentIndex += sequence.length();
		}

		result.data = newData;
		result.offset = 0;
		result.status = SequenceStatus.NORMAL;
		result.length = newData.length;

		return result;
	}

	@Override
	public IDNASequence copy() {
		return this.getSequenceReference(false);
	}

}

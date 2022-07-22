package es.udc.gac.sparkec.sequence;

import java.io.Serializable;

/**
 * The IDNASequence is the base interface that every DNASequence implementation must
 * implement. It defines the DNA bases being used, and default implementations for some
 * utility methods.
 * 
 * <p>
 * Each DNA Sequence must be able to be edited, read, reversed, get their bases switched by
 * their alternates, and RC'ed.
 * 
 * <p>
 * Also, methods that allow method chaining reusing the same DNA Sequence instance are offered by this
 * interface.
 */
public interface IDNASequence extends Iterable<Byte>, Comparable<IDNASequence>, Serializable {

	public static final float RATIO_ISPOLY = 0.9f;
	
	
	public static class DNABases {

		/**
		 * Adenyne base
		 */
		public static final byte A = 'A';

		/**
		 * Thymine base
		 */
		public static final byte T = 'T';

		/**
		 * Cytosine base
		 */
		public static final byte C = 'C';

		/**
		 * Guanine base
		 */
		public static final byte G = 'G';

		/**
		 * Unknown base
		 */
		public static final byte N = 'N';

		/**
		 * Invalid base
		 */
		public static final byte X = 'X';

		private DNABases() {

		}
	}

	/**
	 * Gets the DNA alternate base of a given base.
	 * 
	 * @param base The original base
	 * @return The alternate base
	 */
	public static byte getAlternateBase(byte base) {
		switch (base) {
		case 'A':
			return 'T';
		case 'T':
			return 'A';
		case 'C':
			return 'G';
		case 'G':
			return 'C';
		case 'N':
			return 'N';
		case 'X':
		default:
			return 'X';
		}
	}

	/**
	 * Transforms this Sequence into a byte array.
	 * 
	 * @return The new byte array
	 */
	public byte[] toByteArray();

	/**
	 * Returns a new byte array with the data of this sequence.
	 * A new byte array will get allocated.
	 * 
	 * @return The array with the data
	 */
	public byte[] getRawData();

	/**
	 * Returns the DNA base at a given position.
	 * 
	 * @param pos The position
	 * @return The DNA base
	 */
	public byte getValue(int pos);

	/**
	 * Sets the base of the Sequence at a given position.
	 * 
	 * @param pos   The position
	 * @param value The new base value
	 */
	public void setValue(int pos, byte value);

	/**
	 * Gets the length of this sequence.
	 * 
	 * @return The length of this sequence
	 */
	public int length();

	/**
	 * Returns a new Sequence with only a part of the bases of this one.
	 * 
	 * @param start         The starting base, inclusive.
	 * @param end           The latest base, inclusive
	 * @param reuseSequence Whether the Sequence object reference should be reused
	 * @return The new Sequence
	 */
	public IDNASequence getSubSequence(int start, int end, boolean reuseSequence);

	/**
	 * Returns a new Sequence with only a part of the bases of this one, without
	 * reusing the Sequence object.
	 * 
	 * @param start The starting base, inclusive.
	 * @param end   The latest base, inclusive
	 * @return The new Sequence
	 */
	public default IDNASequence getSubSequence(int start, int end) {
		return this.getSubSequence(start, end, false);
	}

	/**
	 * Returns a new Sequence reversing this one (first base will become latest).
	 * 
	 * @param reuseSequence Whether the Sequence object reference should be reused
	 * @return The new Sequence
	 */
	public IDNASequence reverseSequence(boolean reuseSequence);

	/**
	 * Returns a new Sequence reversing this one (first base will become latest),
	 * without reusing the Sequence object.
	 * 
	 * @return The new Sequence
	 */
	public default IDNASequence reverseSequence() {
		return this.reverseSequence(false);
	}

	/**
	 * Returns a new Sequence with alternate bases of this one.
	 * 
	 * @param reuseSequence Whether the Sequence object reference should be reused
	 * @return The new Sequence
	 */
	public IDNASequence invertSequence(boolean reuseSequence);

	/**
	 * Returns a new Sequence with alternate bases of this one.
	 * 
	 * @return The new Sequence
	 */
	public default IDNASequence invertSequence() {
		return this.invertSequence(false);
	}

	/**
	 * Computes the RC to this Sequence. A Sequence is an RC of another one when it
	 * is both reversed and using alternate bases.
	 * 
	 * @param reuseSequence Whether the Sequence object reference should be reused
	 * @return The new Sequence
	 */
	public IDNASequence rcSequence(boolean reuseSequence);
	
	

	/**
	 * Computes the RC to this Sequence. A Sequence is an RC of another one when it
	 * is both reversed and using alternate bases. This method does not reuse the
	 * Sequence object reference.
	 * 
	 * @return The new Sequence
	 */
	public default IDNASequence rcSequence() {
		return this.rcSequence(false);
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
	public IDNASequence getSubSequenceWithoutMiddleBase(int start, int end, boolean reuseSequence)
			throws UnableToSkipMiddleBase;

	/**
	 * Computes a new Sequence from a part of this one, without taking it's middle
	 * base. This method will not be available to get called again in the new
	 * Sequence. This method will not reuse the Sequence object reference.
	 * 
	 * @param start The index of the first base of the new Sequence, inclusive
	 * @param end   The index of the latest base of the new Sequence, inclusive
	 * @return The new Sequence
	 * @throws UnableToSkipMiddleBase If this Sequence already skipped it's middle
	 *                                base before
	 */
	public default IDNASequence getSubSequenceWithoutMiddleBase(int start, int end) throws UnableToSkipMiddleBase {
		return this.getSubSequenceWithoutMiddleBase(start, end, false);
	}

	@Override
	public default int compareTo(IDNASequence o) {

		int i = 0;
		int j = 0;
		while (true) {
			if (i < this.length()) {
				if (j < o.length()) {

					int compare = Byte.compare(this.getValue(i), o.getValue(j));

					if (compare != 0) {
						return compare;
					}

				} else {
					return 1;
				}
			} else {
				if (j < o.length()) {
					return -1;
				} else {
					return 0;
				}
			}
			i++;
			j++;
		}
	}

	/**
	 * Checks whether this Sequence contains the given base.
	 * 
	 * @param base The base to look for
	 * @return Whether the Sequence did contain the base
	 */
	public boolean containsBase(byte base);

	/**
	 * Appends many sequences to this sequence.
	 * 
	 * @param reuseSequence Whether this sequence reference should be reused
	 * @param sequences      The sequences to append to this sequence
	 * @return The resulting sequence
	 */
	public IDNASequence append(boolean reuseSequence, IDNASequence... sequences);

	/**
	 * Appends many sequences to this sequence, without reusing this sequence
	 * reference.
	 * 
	 * @param sequences The sequences to append to this sequence
	 * @return The resulting sequence
	 */
	public default IDNASequence append(IDNASequence... sequences) {
		return this.append(false, sequences);
	}
	
	/**
	 * Creates a new Sequence object that is a copy of this Sequence
	 * @return The new Sequence
	 */
	public IDNASequence copy();
	
	
	public default boolean isPoly(byte base) {
		byte[] seq = this.toByteArray();
		
		
		int baseCount = 0;
		for (int i = 0; i < seq.length; i++) {
			if (seq[i] == base) {
				baseCount++;
			}
		}
		
		return ((1.0f * baseCount / seq.length) >= RATIO_ISPOLY);
	}
	
}

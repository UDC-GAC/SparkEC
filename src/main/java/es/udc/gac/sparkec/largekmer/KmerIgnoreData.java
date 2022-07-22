package es.udc.gac.sparkec.largekmer;

import java.io.Serializable;

import es.udc.gac.sparkec.sequence.IDNASequence;

/**
 * This class contains the ignore data for a single kmer.
 */
public class KmerIgnoreData implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Whether this is a PinchCorrect filter ignore data. If it is false, then it is SpreadCorrect filter
	 * ignore data.
	 */
	private boolean isIgnP;
	
	/**
	 * The kmer.
	 */
	private IDNASequence kmer;

	/**
	 * Returns whether this is a PinchCorrect filter ignore data. If it is false, then it is 
	 * SpreadCorrect filter ignore data.
	 * @return Whether this is a PinchCorrect filter ignore data. If it is false, then it
	 * is SpreadCorrect filter ignore data
	 */
	public boolean isIgnP() {
		return isIgnP;
	}

	/**
	 * Sets whether this is a PinchCorrect filter ignore data. If it is false, then it is 
	 * SpreadCorrect filter ignore data.
	 * @param isIgnP Whether this is a PinchCorrect filter ignore data. If it is false, then it
	 * is SpreadCorrect filter ignore data
	 */
	public void setIgnP(boolean isIgnP) {
		this.isIgnP = isIgnP;
	}

	/**
	 * Returns the kmer.
	 * @return The kmer
	 */
	public IDNASequence getKmer() {
		return kmer;
	}

	/**
	 * Sets the kmer.
	 * @param kmer The kmer
	 */
	public void setKmer(IDNASequence kmer) {
		this.kmer = kmer;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (isIgnP ? 1231 : 1237);
		result = prime * result + kmer.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KmerIgnoreData other = (KmerIgnoreData) obj;
		if (isIgnP != other.isIgnP)
			return false;
		return kmer.equals(other.kmer);
	}

	@Override
	public String toString() {
		return "KmerIgnoreData [isIgnP=" + isIgnP + ", kmer=" + kmer.toString() + "]";
	}

	/**
	 * Default constructor for KmerIgnoreData.
	 * @param isIgnP Whether this is a PinchCorrect filter ignore data. If it is false, then it
	 * is SpreadCorrect filter ignore data
	 * @param kmer The kmer
	 */
	public KmerIgnoreData(boolean isIgnP, IDNASequence kmer) {
		super();
		this.isIgnP = isIgnP;
		this.kmer = kmer;
	}
}

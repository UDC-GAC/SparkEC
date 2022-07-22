package es.udc.gac.sparkec.spreadcorrect;

import java.io.Serializable;
import java.util.Arrays;

import es.udc.gac.sparkec.sequence.IDNASequence;

/**
 * This class will keep track of the Node data of each kmer apparition.
 */
public class SpreadCorrectKmerReadData implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The ID of the Node.
	 */
	private long nodeId;
	
	/**
	 * Whether the kmer was found reversed.
	 */
	private boolean reversed;
	
	/**
	 * The position of the kmer inside the Sequence.
	 */
	private int pos;
	
	/**
	 * The arm of the kmer.
	 */
	private IDNASequence seq;
	
	/**
	 * The qualities for the arm of the kmer.
	 */
	private byte[] qv;
	
	/**
	 * The offset of the arm.
	 */
	private int offset;
	
	/**
	 * The length of the arm.
	 */
	private int length;

	/**
	 * Returns the ID of the Node.
	 * @return The ID of the Node
	 */
	public long getNodeId() {
		return nodeId;
	}

	/**
	 * Sets the ID of the Node.
	 * @param nodeId The ID of the Node
	 */
	public void setNodeId(long nodeId) {
		this.nodeId = nodeId;
	}

	/**
	 * Returns whether this kmer was found reversed.
	 * @return Whether this kmer was found reversed
	 */
	public boolean isReversed() {
		return reversed;
	}

	/**
	 * Sets whether this kmer was found reversed.
	 * @param reversed Whether this kmer was found reversed
	 */
	public void setReversed(boolean reversed) {
		this.reversed = reversed;
	}
	
	/**
	 * Returns the position of the kmer inside the Node Sequence.
	 * @return The position of the kmer inside the Node Sequence
	 */
	public int getPos() {
		return pos;
	}

	/**
	 * Sets the position of the kmer inside the Node Sequence.
	 * @param pos The position of the kmer inside the Node Sequence
	 */
	public void setPos(int pos) {
		this.pos = pos;
	}

	/**
	 * Returns the arm Sequence.
	 * @return The arm Sequence
	 */
	public IDNASequence getSeq() {
		return seq;
	}

	/**
	 * Sets the arm Sequence.
	 * @param seq The arm Sequence
	 */
	public void setSeq(IDNASequence seq) {
		this.seq = seq;
	}

	/**
	 * Returns the quality values of the arm Sequence.
	 * @return The quality values of the arm Sequence
	 */
	public byte[] getQv() {
		return qv;
	}

	/**
	 * Sets the quality values of the arm Sequence.
	 * @param qv The quality values of the arm Sequence
	 */
	public void setQv(byte[] qv) {
		this.qv = qv;
	}

	/**
	 * Returns the offset of the arm.
	 * @return The offset of the arm
	 */
	public int getOffset() {
		return offset;
	}

	/**
	 * Sets the offset of the arm.
	 * @param offset The offset of the arm
	 */
	public void setOffset(int offset) {
		this.offset = offset;
	}

	/**
	 * Returns the length of the arm.
	 * @return The length of the arm
	 */
	public int getLength() {
		return length;
	}

	/**
	 * Sets the length of the arm.
	 * @param length The length of the arm
	 */
	public void setLength(int length) {
		this.length = length;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + length;
		result = prime * result + Long.hashCode(nodeId);
		result = prime * result + offset;
		result = prime * result + pos;
		result = prime * result + Arrays.hashCode(qv);
		result = prime * result + (reversed ? 1231 : 1237);
		result = prime * result + seq.hashCode();
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
		SpreadCorrectKmerReadData other = (SpreadCorrectKmerReadData) obj;
		if (length != other.length)
			return false;
		if (nodeId != other.nodeId) {
			return false;
		}
		if (offset != other.offset)
			return false;
		if (pos != other.pos)
			return false;
		if (!Arrays.equals(qv, other.qv))
			return false;
		if (reversed != other.reversed)
			return false;
		return seq.equals(other.seq);
	}

	@Override
	public String toString() {
		return "SpreadCorrectKmerReadData [nodeId=" + nodeId + ", reversed=" + reversed + ", pos=" + pos + ", seq="
				+ seq.toString() + ", qv=" + Arrays.toString(qv) + ", offset=" + offset + ", length=" + length + "]";
	}

	/**
	 * Empty constructor for SpreadCorrectKmerReadData.
	 */
	public SpreadCorrectKmerReadData() {
		super();
	}

	/**
	 * Default constructor for SpreadCorrectKmerReadData
	 * 
	 * @param nodeId    The id of the node
	 * @param reversed  Whether the kmer is reversed
	 * @param pos       Position of the kmer
	 * @param arm  		Arm of the kmer
	 * @param kmer      The kmer itself
	 * @param qv        Quality of the arms and the kmer
	 * @param offset    Offset of the kmer
	 * @param length    Length of the sequence
	 */
	public SpreadCorrectKmerReadData(long nodeId, boolean reversed, int pos, IDNASequence arm, IDNASequence kmer, byte[] qv,
			int offset, int length) {
		super();
		this.nodeId = nodeId;
		this.reversed = reversed;
		this.pos = pos;

		this.seq = arm;

		this.qv = qv;
		this.offset = offset;
		this.length = length;
	}

	/**
	 * Returns the left position of the arm.
	 * @return The left position of the arm
	 */
	public int getARMLeft() {
		return (pos - offset);
	}

	/**
	 * Returns the right position of the arm.
	 * @param k The k used by this execution
	 * @return The right position of the arm
	 */
	public int getARMRight(int k) {
		return (seq.length() - k - (pos - offset));
	}

}

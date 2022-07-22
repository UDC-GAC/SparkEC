package es.udc.gac.sparkec.pinchcorrect;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class will keep track of the Node data were each kmer apparition was found at the
 * PinchCorrectRecommendation SubPhase
 */
public class ReadInfo implements Serializable {

	private static final long serialVersionUID = 1L;
	
	/**
	 * The Node ID.
	 */
	private long id;
	
	/**
	 * Whether the kmer was found reversed.
	 */
	private boolean dir;
	
	/**
	 * The position where the kmer was found.
	 */
	private int pos;
	
	/**
	 * The length of the Node Sequence where the kmer was found.
	 */
	private short length;
	
	/**
	 * The middle base of the kmer.
	 */
	private char base;
	
	/**
	 * The quality value of the middle base of the kmer.
	 */
	private char qv;

	/**
	 * Default constructor for ReadInfo.
	 * @param id1 The Node ID
	 * @param dir Whether the kmer was found reversed
	 * @param pos1 The position of the kmer
	 * @param base1 The middle base of the kmer
	 * @param qv1 The quality value of the kmer
	 * @param length1 The Node Sequence length
	 */
    public ReadInfo(long id1, boolean dir, short pos1, char base1,
                    char qv1, final short length1)  {
    		this.dir = dir;
            id = id1;
            pos = pos1;
            base = base1;
            length = length1;
            qv = qv1;
    }
    
    @Override
    public boolean equals(Object other){
        if (other instanceof ReadInfo){
            return this.id == ((ReadInfo) other).id;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + Objects.hashCode(this.id);
        return hash;
    }
    
    @Override
    public String toString(){
        String d = dir ? "f" : "r";
        return id+"\t"+d+"\t"+pos+"\t"+base+"\t"+qv+"\t"+length;
    }

    /**
     * Returns the Node ID.
     * @return The Node ID
     */
	public long getId() {
		return id;
	}

	/**
	 * Sets the Node ID.
	 * @param id The Node ID
	 */
	public void setId(long id) {
		this.id = id;
	}

	/**
	 * Checks whether the kmer was found reversed.
	 * @return Whether the kmer was found reversed
	 */
	public boolean isDir() {
		return dir;
	}

	/**
	 * Sets whether the kmer was found reversed.
	 * @param dir Whether the kmer was found reversed
	 */
	public void setDir(boolean dir) {
		this.dir = dir;
	}

	/**
	 * Returns the position of the kmer.
	 * @return The position of the kmer
	 */
	public int getPos() {
		return pos;
	}

	/**
	 * Sets the position of the kmer.
	 * @param pos The position of the kmer
	 */
	public void setPos(int pos) {
		this.pos = pos;
	}

	/**
	 * Returns the length of the Node Sequence.
	 * @return The length of the Node Sequence
	 */
	public short getLength() {
		return length;
	}

	/**
	 * Sets the length of the Node Sequence.
	 * @param length The length of the Node Sequence
	 */
	public void setLength(short length) {
		this.length = length;
	}

	/**
	 * Returns the middle base of the kmer.
	 * @return The middle base of the kmer
	 */
	public char getBase() {
		return base;
	}

	/**
	 * Sets the middle base of the kmer.
	 * @param base The middle base of the kmer
	 */
	public void setBase(char base) {
		this.base = base;
	}

	/**
	 * Returns the quality value of the middle base of the kmer.
	 * @return The quality value of the middle base of the kmer
	 */
	public char getQv() {
		return qv;
	}

	/**
	 * Sets the quality value of the middle base of the kmer.
	 * @param qv The quality value of the middle base of the kmer
	 */
	public void setQv(char qv) {
		this.qv = qv;
	}
    
    
}

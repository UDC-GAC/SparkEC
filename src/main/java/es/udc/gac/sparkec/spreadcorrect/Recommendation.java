package es.udc.gac.sparkec.spreadcorrect;

import java.io.Serializable;

/**
 * This class contains all the information about a single recommendation being made for a base.
 */
public class Recommendation implements Serializable {

	
	private static final long serialVersionUID = 1L;
	
	/**
	 * The base recommended.
	 */
	private final char base;
	
	/**
	 * The position of the recommendation.
	 */
    private final int pos;
    
    /**
     * Default constructor for a Recommendation.
     * @param base The base recommended
     * @param pos The position of the recommendation
     */
    public Recommendation(char base,int pos){
        this.base = base;
        this.pos = pos;
    }

	@Override
	public String toString() {
		return "(" + base + ", " + pos + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + base;
		result = prime * result + pos;
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
		Recommendation other = (Recommendation) obj;
		if (base != other.base)
			return false;
		return pos == other.pos;
	}

	/**
	 * Returns the base recommended.
	 * @return The base recommended
	 */
	public char getBase() {
		return base;
	}

	/**
	 * Returns the position of the recommendation.
	 * @return The position of the recommendation
	 */
	public int getPos() {
		return pos;
	}
    
}

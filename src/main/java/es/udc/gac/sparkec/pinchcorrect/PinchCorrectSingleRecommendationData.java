package es.udc.gac.sparkec.pinchcorrect;

import java.io.Serializable;

/**
 * This class contains information about a single Recommendation being made at the PinchCorrect Phase.
 */
public class PinchCorrectSingleRecommendationData implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The recommended base.
	 */
	private byte recommendedBase;
	
	/**
	 * The position of the recommendation.
	 */
	private int pos;

	/**
	 * Returns the recommended base.
	 * @return The recommended base
	 */
	public byte getRecommendedBase() {
		return recommendedBase;
	}

	/**
	 * Sets the value of the recommendedBase.
	 * @param recommendedBase The new value for the recommendedBase
	 */
	public void setRecommendedBase(byte recommendedBase) {
		this.recommendedBase = recommendedBase;
	}

	/**
	 * Returns the value of the position of the recommendation.
	 * @return The value of the position of the recommendation.
	 */
	public int getPos() {
		return pos;
	}

	/**
	 * Sets the value of the position of the recommendation.
	 * @param pos The new value for the position of the recommendation
	 */
	public void setPos(int pos) {
		this.pos = pos;
	}

	@Override
	public String toString() {
		return "PinchCorrectSingleRecommendationData [recommendedBase=" + recommendedBase + ", pos=" + pos + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + pos;
		result = prime * result + recommendedBase;
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
		PinchCorrectSingleRecommendationData other = (PinchCorrectSingleRecommendationData) obj;
		if (pos != other.pos)
			return false;
		if (recommendedBase != other.recommendedBase)
			return false;
		return true;
	}

	/**
	 * Default constructor for PinchCorrectSingleRecommendationData.
	 * @param recommendedBase The recommended base
	 * @param pos The position of the recommendation
	 */
	public PinchCorrectSingleRecommendationData(byte recommendedBase, int pos) {
		super();
		this.recommendedBase = recommendedBase;
		this.pos = pos;
	}

}

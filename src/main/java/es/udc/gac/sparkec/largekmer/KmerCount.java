package es.udc.gac.sparkec.largekmer;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import es.udc.gac.sparkec.node.Node;

/**
 * This class will contain the Node data with the ignore values for the Node.
 */
public class KmerCount implements Serializable {

	private static final long serialVersionUID = 1L;
	
	/**
	 * The Node.
	 */
	public Node node;
	
	/**
	 * The PinchCorrect filter ignore values.
	 */
	public Set<Integer> count_P;
	
	/**
	 * The SpreadCorrect filter ignore values.
	 */
	public Set<Integer> count_F;

	/**
	 * Default constructor for KmerCount.
	 */
	public KmerCount() {
		node = new Node();
		count_P = new HashSet<>();
		count_F = new HashSet<>();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((count_F == null) ? 0 : count_F.hashCode());
		result = prime * result + ((count_P == null) ? 0 : count_P.hashCode());
		result = prime * result + ((node == null) ? 0 : node.hashCode());
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
		KmerCount other = (KmerCount) obj;
		if (count_F == null) {
			if (other.count_F != null)
				return false;
		} else if (!count_F.equals(other.count_F))
			return false;
		if (count_P == null) {
			if (other.count_P != null)
				return false;
		} else if (!count_P.equals(other.count_P))
			return false;
		if (node == null) {
			if (other.node != null)
				return false;
		} else if (!node.equals(other.node))
			return false;
		return true;
	}

}

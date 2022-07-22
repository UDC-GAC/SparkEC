package es.udc.gac.sparkec.node;

/**
 * Exception used when a parsing error happened while reading a Node from one of its
 * many String representations.
 */
public class InvalidNodeFormatException extends Exception {

	private static final long serialVersionUID = 1L;

	public InvalidNodeFormatException() {
		super("Invalid node format!");
	}
}

package es.udc.gac.sparkec;

/**
 * Exception thrown when a unexistent time measurement is being queried.
 */
public class InvalidTimeException extends Exception {

	private static final long serialVersionUID = 1L;
	
	/**
	 * The name of the time measurement queried.
	 */
	private final String name;
	
	/**
	 * Returns the name of the time measurement queried.
	 * @return The name of the time measurement queried
	 */
	public String getName() {
		return this.name;
	}
	
	/**
	 * Default constructor for the InvalidTimeException.
	 * @param name The name of the time measurement being queried
	 */
	public InvalidTimeException(String name) {
		this.name = name;
	}
}

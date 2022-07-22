package es.udc.gac.sparkec.split;

/**
 * This exception is thrown when a phase split strategy is being used before being initialized.
 * In order to estimate the splits needed for the phases is necessary to first get stats about the
 * DataSet being currently processed.
 */
public class UninitializedPhaseSplitStrategyException extends RuntimeException {

	private static final long serialVersionUID = 1L;

}

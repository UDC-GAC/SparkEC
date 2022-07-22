
package es.udc.gac.sparkec;

/**
 * Default interface for each Phase that can be run through SparkEC.
 */
public interface Phase {
    
	/**
	 * Runs the current phase, updating the data container after that process.
	 * @param data The RDD storage container that provides both the input datasets, and allows to include the 
	 * output dataset.
	 */
    public void runPhase(Data data);
    
    /**
     * Returns an unique name for this phase
     * @return The unique phase name
     */
    public String getPhaseName();
    
    /**
     * Prints to the console all the stats (if there are any) of the latest phase execution.
     */
    public void printStats();
    
}

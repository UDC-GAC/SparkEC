package es.udc.gac.sparkec;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Class used to store and measure times for many computations, each one with an unique
 * name assigned.
 */
public class TimeMonitor implements Iterable<Map.Entry<String, Float>> {
	
	/**
	 * The timestamp of the moment each measure started.
	 */
	private final Map<String, Long> startingTimes;
	
	/**
	 * The duration for each measure.
	 */
	private final Map<String, Float> measured;
	
	/**
	 * Default constructor for the TimeMonitor.
	 */
	public TimeMonitor() {
		startingTimes = new HashMap<>();
		measured = new HashMap<>();
	}
	
	/**
	 * Returns an iterator for each measured time.
	 */
	@Override
	public Iterator<Map.Entry<String, Float>> iterator(){
		return measured.entrySet().iterator();
	}
	
	/**
	 * Starts a new measurement with a name.
	 * @param name The name of the measurement being made
	 */
	public void startMeasuring(String name) {
		startingTimes.put(name, System.currentTimeMillis());
	}
	
	/**
	 * Ends the measurement of a time linked with a given name.
	 * @param name The name of the measurement
	 * @throws InvalidTimeException When there was no measurement associated to this name
	 */
	public void finishMeasuring(String name) throws InvalidTimeException {
		Long now = System.currentTimeMillis();
		Long starting = startingTimes.get(name);
		
		if (starting == null) {
			throw new InvalidTimeException(name);
		}
		
		measured.put(name, (now - starting)/1000.0f);
		
	}
	
	/**
	 * Gets the time measured for a given name.
	 * @param name The name of the measure
	 * @return The time measured
	 * @throws InvalidTimeException When there was no measure associated to this name
	 */
	public float getMeasurement(String name) throws InvalidTimeException {
		Float out = measured.get(name);
		
		if (out == null) {
			throw new InvalidTimeException(name);
		}
		
		return out;
	}
}

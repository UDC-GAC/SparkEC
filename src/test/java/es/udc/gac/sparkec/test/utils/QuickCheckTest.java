package es.udc.gac.sparkec.test.utils;

import es.udc.gac.sparkec.split.IPhaseSplitStrategy;
import es.udc.gac.sparkec.split.PhaseSplitStrategy;

public abstract class QuickCheckTest {
	
	private IPhaseSplitStrategy splitStrategy;
	
	protected IPhaseSplitStrategy getSplitStrategy() {
		if (splitStrategy == null) {
			splitStrategy = new PhaseSplitStrategy(24, 8L * 1024L * 1024L * 1024L, 5.25f);
			splitStrategy.initialize(100, 7000);
		}
		return splitStrategy;
	}
	
}

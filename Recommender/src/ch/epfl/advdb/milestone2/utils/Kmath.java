package ch.epfl.advdb.milestone2.utils;

import java.util.Random;

public class Kmath {
	static Random rand=null;
	
	public static double getRandom() {
		if(rand==null) 
			rand = new Random(System.currentTimeMillis());
		return rand.nextDouble();
	}
	
	public static int getRandom(int end) {
		if(rand==null)
			rand = new Random(System.currentTimeMillis());
		return rand.nextInt(end)+1;
	}
}

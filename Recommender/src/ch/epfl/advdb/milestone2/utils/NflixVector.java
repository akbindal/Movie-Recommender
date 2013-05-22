package ch.epfl.advdb.milestone2.utils;

import org.apache.hadoop.io.FloatWritable;

public class NflixVector {
	public static void add(FloatArrayWritable vect, float[] center) {
		FloatWritable[] x = (FloatWritable[]) vect.toArray();
		for(int i =0 ; i<10; i++) {
			center[i]+=x[i].get();
		}
	}
}

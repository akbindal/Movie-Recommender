package ch.epfl.advdb.milestone2.utils;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;

public class ImdbVector {
	
	//add intarrayWritable to the map(integer, float)
	public static int add(IntArrayWritable vec, 
			Map<Integer, Float> sum) {
		IntWritable[] x = (IntWritable[]) vec.toArray();
		int key;
		float val, agg;
		for(int i =0; i<x.length; i++) {
			key = x[i].get();
			val = 1;
			if(sum.containsKey(key)) {
				agg = sum.get(key);
				sum.put(key, agg+val);
			} else 
				sum.put(key, val);
		}
		return x.length;
	}

	public static float getLength(Map<Integer, Float> center) {
		float len = (float) 0.00;
		for(Entry<Integer, Float> e: center.entrySet()) {
			len+=e.getValue()*e.getValue();
		}
		return (float)Math.sqrt(len);
	}

	public static void takeAvg(Map<Integer, Float> center, int count) {
		// TODO Auto-generated method stub
		float agg=(float)0.00;
		for(Integer e: center.keySet()) {
			agg= center.get(e)/count;
			center.put(e, agg);
		}
	}
	public static void normalize(Map<Integer, Float> vect, float length) {
		// TODO Auto-generated method stub
		float agg=(float)0.00;
		for(Integer e: vect.keySet()) {
			agg= vect.get(e)/length;
			vect.put(e, agg);
		}
	}
}

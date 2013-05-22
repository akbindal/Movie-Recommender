package ch.epfl.advdb.milestone2.utils;

import java.util.Collection;


import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class FloatArrayWritable extends ArrayWritable{
	public FloatArrayWritable() {
		super(FloatWritable.class);
	}
	
	/**
	 * Sets the writable content from a array of String objects.
	 * takes care of duplicate values
	 * @param texts A Collection of String objects.
	 */
	
	public FloatArrayWritable setIntFloatCollection(
			Collection<IntFloatPair> in) {
		FloatWritable[] floatArray = new FloatWritable[11];
		for(IntFloatPair pair: in) {
			int index = pair.getFirst();
			float val = pair.getSecond();
			floatArray[index]=new FloatWritable(val);
		}		
		// set array
		this.set(floatArray);
		return this;
	}
	
	public FloatArrayWritable setFloatArray(float[] f) {
		FloatWritable[] floatArray = new FloatWritable[10];
		try {
		for(int i =0; i<10; i++) {
			floatArray[i]= new FloatWritable(f[i]);
		}
		} catch (Exception e) {
			e.printStackTrace();
		}
		// set array
		this.set(floatArray);
		return this;
	}
	
	@Override
	public int hashCode() {
		int hashCode = 0;
		for (Writable el: this.get()) {
			hashCode += el.toString().hashCode();
		}
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof FloatArrayWritable) {
			FloatArrayWritable other = (FloatArrayWritable)obj;
			Writable[] myEls = this.get();
			Writable[] otherEls = other.get();
			
			if (otherEls.length != myEls.length) {
				return false;
			} else {
				for (int i = 0; i < myEls.length; ++i) {
					if (!myEls[i].equals(otherEls[i])) {
						return false;
					}
				}
				return true;
			}
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Writable el: this.get()) {
			if (sb.length() > 0) {
				sb.append(", ");
			}
			sb.append(el.toString());
		}
		return sb.toString();
	}
	

}

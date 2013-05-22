package ch.epfl.advdb.milestone2.utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntFloatPairArray extends ArrayWritable {
	public IntFloatPairArray() {
		super(IntFloatPair.class);
	}
	
	/**
	 * Sets the writable content from a array of String objects.
	 * takes care of duplicate values
	 * @param texts A Collection of String objects.
	 */
	
	//used for single centers
	public IntFloatPairArray setIntWritable(IntWritable[] ints) {
		// convert to an array of Text
		IntFloatPair[] intFloatArray = new IntFloatPair[ints.length];
		for(int i =0 ;i<ints.length; i++) {
			intFloatArray[i] = new IntFloatPair(ints[i].get(), (long)1.0);
		}
		// set array
		this.set(intFloatArray);
		
		return this;
	}
	
	public IntFloatPairArray setEntitySet(Set<Entry<Integer, Float>> 
								entry) {
		// convert to an array of intfloatpair
		IntFloatPair[] intFloatArray = new IntFloatPair[entry.size()];
		
		int i = 0;
		for (Entry<Integer, Float> e: entry) {
			intFloatArray[i] = new IntFloatPair(e.getKey(), e.getValue());
			++i;
		}
		
		// set array
		this.set(intFloatArray);
		
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
		if (obj instanceof IntArrayWritable) {
			IntArrayWritable other = (IntArrayWritable)obj;
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

package ch.epfl.advdb.milestone2.utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable{
	public IntArrayWritable() {
		super(IntWritable.class);
	}
	
	/**
	 * Sets the writable content from a array of String objects.
	 * takes care of duplicate values
	 * @param texts A Collection of String objects.
	 */
	
	public IntArrayWritable setStringArray (String[] stringInts, 
			int startIndex) {
		int arrayLength = stringInts.length;
		
		Set<Integer> ints = new HashSet<Integer>();
		for (int i = startIndex; i<arrayLength; i++) {
			int ele = Integer.parseInt(stringInts[i]);
			ints.add(ele);
		}		
		
		IntWritable[] intArray = new IntWritable[arrayLength-startIndex];
		
		int pos = 0;
		for (Integer in: ints) {
			intArray[pos] = new IntWritable(in);
			pos++;
		}
		
		// set array
		this.set(intArray);
		return this;
	}
	
	public IntArrayWritable setIntegerCollection(Collection<Integer> ints) {
		// convert to an array of Text
		IntWritable[] intArray = new IntWritable[ints.size()];
		
		int i = 0;
		for (int element: ints) {
			intArray[i] = new IntWritable(element);
			++i;
		}
		
		// set array
		this.set(intArray);
		
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

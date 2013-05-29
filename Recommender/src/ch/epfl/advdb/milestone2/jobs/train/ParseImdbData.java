package ch.epfl.advdb.milestone2.jobs.train;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import ch.epfl.advdb.milestone2.config.Constant;
import ch.epfl.advdb.milestone2.utils.FloatArrayWritable;
import ch.epfl.advdb.milestone2.utils.ImdbVector;
import ch.epfl.advdb.milestone2.utils.IntArrayWritable;
import ch.epfl.advdb.milestone2.utils.IntFloatPair;
import ch.epfl.advdb.milestone2.utils.IntFloatPairArray;

/**
 * convert text input format to sequential file format
 * @author ashish
 *
 */
public class ParseImdbData {
	
	static class DataimdbMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, IntWritable, IntArrayWritable>{
		private IntArrayWritable opValue = new IntArrayWritable();
		private IntWritable opKey = new IntWritable();
		
		@Override
		public void map(LongWritable inKey, Text inValue,
				OutputCollector<IntWritable, IntArrayWritable> output,
				Reporter arg3) throws IOException {
			//convert text to vector and send
			
			String vect = inValue.toString();
			String[] tokens = vect.split(",");
			if(tokens.length<2) return; //no features
			
			//float length = (float) Math.sqrt(vec.size()); //get the size of vector
			opKey.set(Integer.parseInt(tokens[0]));
			opValue.setStringArray(tokens, 1);
			if(opKey==null || opValue==null) 
				System.out.println("Falut->"+inValue);
			try {
			output.collect(opKey, opValue);
			} catch (Exception e) {
				System.out.println("Falut->"+inValue);
			}
		}	
	}
//	no need, reducer set to 0
//	static class IdentityReducerextends extends MapReduceBase implements Reducer {
//
//		@Override
//		public void reduce(Object arg0, Iterator arg1, OutputCollector arg2,
//				Reporter arg3) throws IOException {
//			// TODO Auto-generated method stub
//			
//		}
//	}
	
	public static JobConf getJobConfig(Configuration config, Class class1, 
			String inPath, String opPath) {
		JobConf conf = new JobConf(config, class1);
		conf.setJobName("IMDB data parser");
		
		//configure mapper
		conf.setMapperClass(DataimdbMapper.class);
		conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(IntArrayWritable.class);
        
        //confgiure the reducer
        /** no need identitiy reducer**/
		//conf.setReducerClass(IdentityReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntArrayWritable.class);
		
		if(!Constant.DEBUG) {
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setCompressOutput(conf, true);
			SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.RECORD);
		}
		
		FileInputFormat.addInputPath(conf, new Path(inPath));
		FileOutputFormat.setOutputPath(conf, new Path(opPath));

		conf.setNumReduceTasks(0);
		
		//Delete the output.
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			fs.delete(new Path(opPath), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conf;
	}
	
}

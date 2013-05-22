package ch.epfl.advdb.milestone2.jobs.train;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import ch.epfl.advdb.milestone2.config.Constant;
import ch.epfl.advdb.milestone2.utils.FloatArrayWritable;
import ch.epfl.advdb.milestone2.utils.IntFloatPair;

public class ParseNflixData {
	
	static class DataNflixMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, 
		IntWritable, IntFloatPair>{
		
		IntFloatPair opValue = new IntFloatPair();
		@Override
		public void map(LongWritable inKey, Text inValue,
				OutputCollector<IntWritable, IntFloatPair> output,
				Reporter arg3) throws IOException {
			//convert text to vector and send
			String vect = inValue.toString();
			//v,fid, mid, value
			String[] tokens = vect.split(",");
			if(!tokens[0].equals("V")) return; //sometime have E
			
			IntWritable outputKey = new IntWritable(
					Integer.parseInt(tokens[2])
					);
			
			int feat = Integer.parseInt(tokens[1]);
			float val = Float.parseFloat(tokens[3]);
			opValue.set(feat,val);
			output.collect(outputKey, opValue);
		}

	}
	
	static class DataNflixReducer extends MapReduceBase 
		implements Reducer<IntWritable, IntFloatPair, 
			IntWritable, FloatArrayWritable>  {
		FloatArrayWritable opVal = new FloatArrayWritable();
		@Override
		public void reduce(IntWritable inKey, Iterator<IntFloatPair> inVal,
				OutputCollector<IntWritable, FloatArrayWritable> output,
				Reporter arg3) throws IOException {
			float[] vec = new float[10]; //0, 10
			IntFloatPair fp;
			int index;
			Float val;
			while(inVal.hasNext()) {
				fp = inVal.next();
				index = fp.getFirst();
				val = fp.getSecond();
				vec[index-1]=val;
			}
			opVal.setFloatArray(vec);
			output.collect(inKey, opVal);
		}
		
	}
		
	public static JobConf getJobConfig(Configuration config, Class class1, 
			String inPath, String opPath) {
		JobConf conf = new JobConf(config, class1);
		conf.setJobName("V sequntial Data ");

		//configure the mapper
		conf.setMapperClass(DataNflixMapper.class);
		conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(IntFloatPair.class);
        
        //confgiure the reducer
		conf.setReducerClass(DataNflixReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(FloatArrayWritable.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(conf, true);
        SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
		
        conf.setNumReduceTasks(Constant.NO_REDUCER);
		
		FileInputFormat.addInputPath(conf, new Path(inPath));
		FileOutputFormat.setOutputPath(conf, new Path(opPath));
		
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

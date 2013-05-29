package ch.epfl.advdb.milestone2.jobs.train;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import ch.epfl.advdb.milestone2.config.Constant;
import ch.epfl.advdb.milestone2.utils.ImdbVector;
import ch.epfl.advdb.milestone2.utils.IntArrayWritable;
import ch.epfl.advdb.milestone2.utils.IntFloatPair;
import ch.epfl.advdb.milestone2.utils.IntFloatPairArray;
import ch.epfl.advdb.milestone2.utils.Kmath;

/**
 * initialize K initial cluster centers
 * @author ashish
 *
 */
public class InitKimdb {
	static int K = Constant.K;
	
	static class InitKMapper extends MapReduceBase
		implements Mapper<IntWritable, IntArrayWritable, 
		IntWritable, IntArrayWritable> {
		IntWritable outputKey=new IntWritable();
		@Override
		public void map(IntWritable inKey, IntArrayWritable inValue,
				OutputCollector<IntWritable, IntArrayWritable> output, 
				Reporter arg3)
				throws IOException {
			outputKey.set(inKey.get()%K);
			output.collect(outputKey, inValue);
		}

	}
	
	/*
	 * Input<Key, <Value>*>: <movieId, <f1,f2..>*>
	 * output<userId:movieId1,Norm_rating1:movieId2,Norm_rating2:movieId3,Norm_rating3:...>
	 * (* :: list)
	 */
	static class InitKReducer extends MapReduceBase implements 
	Reducer<IntWritable, IntArrayWritable, IntWritable, IntFloatPairArray>  {
		private IntFloatPairArray opValue = new IntFloatPairArray();
		private IntWritable opKey = new IntWritable();
		
		private Map<Integer, Float> center = new HashMap<Integer, Float>();

		@Override
		public void reduce(IntWritable key, Iterator<IntArrayWritable> values,
				OutputCollector<IntWritable, IntFloatPairArray> output, Reporter reporter)
				throws IOException {
			center.clear();
			int count =0;
			IntArrayWritable defaultCenter=null;
			while(values.hasNext()) {
				if(Kmath.getRandom()< Constant.K_INITIALIZATION) {
					ImdbVector.add(values.next(), center);
					count++;
				} else defaultCenter=values.next();
			}
			if(count==0) {
				ImdbVector.add(defaultCenter, center);
				//Kmath.getRandInteger(values)*
				opKey.set(key.get());
				opValue.setEntitySet(center.entrySet());
				output.collect(opKey, opValue);
			}else {
				ImdbVector.takeAvg(center, count);
				opKey.set(key.get());
				opValue.setEntitySet(center.entrySet());
				output.collect(opKey, opValue);
			}
		}
	}
	
	public static JobConf getJobConfig(Configuration config, Class class1, 
			String inPath, String opPath) {
		JobConf conf = new JobConf(config, class1);
		conf.setJobName("IMDB K-Initialization");
		
		 // Create the Hadoop job.
		if(!Constant.DEBUG)
		conf.setInputFormat(SequenceFileInputFormat.class);
		

        // Configure the mapper.
		conf.setMapperClass(InitKMapper.class);
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(IntArrayWritable.class);

        // Configure the reducer.
        conf.setReducerClass(InitKReducer.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntFloatPairArray.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(conf, true);
        SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.RECORD);
        
        conf.setNumReduceTasks(Constant.NO_REDUCER);
        
        // Define input and output folders.
        FileInputFormat.addInputPath(conf, new Path(inPath));
        FileOutputFormat.setOutputPath(conf, new Path(opPath));
        
        try {
			FileSystem fs = FileSystem.get(conf);
			//Delete the output.
			fs.delete(new Path(opPath), true);
        } catch (Exception e) {
        	e.printStackTrace();
        }
        return conf;
	}

}

package ch.epfl.advdb.milestone2.jobs.train;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
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

public class StepKimdb {
	

	public static class StepKMapper extends MapReduceBase
		implements Mapper<IntWritable, IntArrayWritable, 
		IntWritable, IntArrayWritable> {
	//	Map<Interger, Float>
		ArrayList<Map<Integer, Float>> centers = new ArrayList<Map<Integer,Float>>(Constant.K);
		boolean[] chkCenter = new boolean[Constant.K];

		@Override
		public void configure(JobConf job)  {
			
			Path[] cacheFiles;
			try {
				cacheFiles = DistributedCache.getLocalCacheFiles(job);
				//System.out.println(cacheFiles.toString());
				if (null != cacheFiles && cacheFiles.length > 0) {
					FileSystem fs = FileSystem.get(job);
			    	for (Path cachePath : cacheFiles) {
			    		System.out.println(cachePath.toString());
			    		loadCenter(fs, cachePath, job);
			        }
			    }
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		}
		
		//<V,fi,movieid,value>
		public void loadCenter(FileSystem fs, Path  path, Configuration conf){
			SequenceFile.Reader reader ;
			try {
			//initializae centers with number of empty vector
			for(int i =0 ;i<Constant.K;i++) {
				Map<Integer, Float> centerVect = new HashMap<Integer, Float>();
				centers.add(centerVect);
			}
			reader = new SequenceFile.Reader(fs, path, conf);
			IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
			IntFloatPairArray value = (IntFloatPairArray) reader.getValueClass().newInstance();
			while(reader.next(key, value))	{
				int centerId = key.get();
				chkCenter[centerId] = true;
				Map<Integer, Float> centerVect = centers.get(centerId);
				IntFloatPair[] ifp = (IntFloatPair[]) value.toArray();
				for(int i=0; i<ifp.length; i++) {
					centerVect.put(ifp[i].getFirst(), ifp[i].getSecond());
				}
				//centers.set(centerId, centerVect);
			}
			
			//generate new center for empty ones
			for(int i =0 ;i<Constant.K;i++) {
				if(!chkCenter[i]) {
					Map<Integer, Float> centerVect = centers.get(i);
					for(int j=0; j<10;j++) {
						int fet = Kmath.getRandom(Constant.FEATURES);
						centerVect.put(fet, (float)1);
					}
				}
			}
			reader.close();
			} catch(Exception e) {
				System.out.println("WARNING: No "+path.toString()+":Couldn't read cache file:"+e.toString());
			}
		}
		IntWritable opKey = new IntWritable();
		
		@Override
		public void map(IntWritable inKey, IntArrayWritable inValue,
				OutputCollector<IntWritable, IntArrayWritable> output, 
				Reporter arg3)
				throws IOException {
			//inkey is movieid
			//invalue is features
			//find the minimum value
			float dist = 0;
			float minDist = 898989;
			int minCenter = -1;
			for(int i=0; i<Constant.K; i++) {
				dist = getDistance(i, inValue);
				if(dist<minDist) {
					minCenter = i;
					minDist = dist;
				}
			}
			opKey.set(minCenter);
			output.collect(opKey, inValue);
		}
		
		//distance should count only feature center has (both has the feature)
		float getDistance(int ci, IntArrayWritable vect) {
			IntWritable[] fet = (IntWritable[]) vect.toArray();
			float distance =0;
			for(int i =0 ;i<fet.length; i++) {
				if(centers.get(ci).containsKey(fet[i].get())) {
					float val = centers.get(ci).get(fet[i].get());
					distance += (val-1)*(val-1);
				} else {
					distance+=1;
				}
			}

			return distance;
		}
	}
	
	static class StepKReducer extends MapReduceBase implements 
	Reducer<IntWritable, IntArrayWritable, IntWritable, IntFloatPairArray>  {
		
		private IntFloatPairArray opValue = new IntFloatPairArray();
		private Map<Integer, Float> center = new HashMap<Integer, Float>();

		@Override
		public void reduce(IntWritable key, Iterator<IntArrayWritable> values,
				OutputCollector<IntWritable, IntFloatPairArray> output, Reporter reporter)
				throws IOException {
			center.clear();
			int count =0;
			while(values.hasNext()) {
				int valLen = ImdbVector.add(values.next(), center);
				if(valLen>0)
					count++;
			}
			if(count>0) {
				ImdbVector.takeAvg(center, count);
			} 
			opValue.setEntitySet(center.entrySet());
			output.collect(key, opValue);
		}
	}
	
	public static JobConf getJobConfig(Configuration config, Class class1, 
			String inPath, String opPath, String cachePath) {
		JobConf conf = new JobConf(config, class1);
		conf.setJobName("IMDB Iter :"+opPath);
		
		 // Create the Hadoop job.
		if(!Constant.DEBUG)
		conf.setInputFormat(SequenceFileInputFormat.class);
		

        // Configure the mapper.
		conf.setMapperClass(StepKMapper.class);
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(IntArrayWritable.class);

        // Configure the reducer.
        conf.setReducerClass(StepKReducer.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntFloatPairArray.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(conf, true);
        SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.RECORD);
        
        conf.setNumReduceTasks(Constant.NO_REDUCER);
        
        // Define input and output folders.
        FileInputFormat.addInputPath(conf, new Path(inPath));
        FileOutputFormat.setOutputPath(conf, new Path(opPath));
        
        FileSystem fs;        
        try {
			fs = FileSystem.get(conf);
			//Delete the output.
			fs.delete(new Path(opPath), true);
			FileStatus[] filestatus = fs.listStatus(new Path(cachePath));
			for (FileStatus status : filestatus) {
			    DistributedCache.addFileToClassPath(status.getPath(), conf);
			}
        } catch (Exception e) {
        	e.printStackTrace();
        }
       
        return conf;
	}

}

package ch.epfl.advdb.milestone2.jobs.train;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import ch.epfl.advdb.milestone2.jobs.train.StepKimdb.StepKMapper;
import ch.epfl.advdb.milestone2.jobs.train.StepKimdb.StepKReducer;
import ch.epfl.advdb.milestone2.utils.ImdbVector;
import ch.epfl.advdb.milestone2.utils.IntArrayWritable;
import ch.epfl.advdb.milestone2.utils.IntFloatPair;
import ch.epfl.advdb.milestone2.utils.IntFloatPairArray;
import ch.epfl.advdb.milestone2.utils.Kmath;

public class FinalKImdb {
	public static class FinalKMapper extends MapReduceBase
		implements Mapper<IntWritable, IntArrayWritable, 
	IntWritable, IntWritable> {

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
				//System.out.println("key="+key+"\t value="+value);
				int centerId = key.get();
				chkCenter[centerId] = true;
				Map<Integer, Float> centerVect = centers.get(centerId);
				IntFloatPair[] ifp = (IntFloatPair[]) value.toArray();
				for(int i=0; i<ifp.length; i++) {
					centerVect.put(ifp[i].getFirst(), ifp[i].getSecond());
				}
				//centers.set(centerId, centerVect);
			}
			
			// don't generate new center for empty ones
//			for(int i =0 ;i<Constant.K;i++) {
//				if(!chkCenter[i]) {
//					Map<Integer, Float> centerVect = centers.get(i);
//					for(int j=0; j<10;j++) {
//						int fet = Kmath.getRandom(Constant.FEATURES);
//						centerVect.put(fet, (float)1);
//					}
//				}
//			}
			reader.close();
			} catch(Exception e) {
				System.out.println("WARNING: No "+path.toString()+":Couldn't read cache file:"+e.toString());
			}
		}
		IntWritable opKey = new IntWritable();
		
		@Override
		public void map(IntWritable inKey, IntArrayWritable inValue,
				OutputCollector<IntWritable, IntWritable> output, 
				Reporter arg3)
				throws IOException {
			//inkey is movieid
			//invalue is features
			//find the minimum distance value
			//if center is empty then distance = infinity ;in last iteration we have removed the mapping
			//so it ensure no movie is mapped to empty cluster;
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
			output.collect(opKey, inKey);
		}
		
		//distance should count only feature center has (both has the feature)
		public float getDistance(int ci, IntArrayWritable vect) {
			IntWritable[] fet = (IntWritable[]) vect.toArray();
			float distance =0;
			Map<Integer, Float> mp = centers.get(ci);
			if(mp.size()<1) return Float.MAX_VALUE;
			for(int i =0 ;i<fet.length; i++) {
				if(mp.containsKey(fet[i].get())) {
					float val = mp.get(fet[i].get());
					distance += (val-1)*(val-1);
				} else {
					distance+=1;
				}
			}
			return distance;
		}
	}
	
	static class FinalKReducer extends MapReduceBase implements 
	Reducer<IntWritable, IntWritable, IntWritable, IntArrayWritable>  {
		
		private IntArrayWritable opValue = new IntArrayWritable();
	
		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, IntArrayWritable> output, Reporter reporter)
				throws IOException {
			List<IntWritable> val = new ArrayList<IntWritable>();
			int count =0;
			while(values.hasNext()) {
				int in = values.next().get();
				IntWritable x = new IntWritable(in);
				val.add(x);
				count++;
			}
			IntWritable[] valArray = new IntWritable[count];
			for(int i =0 ;i<count; i++) {
				valArray[i]=val.get(i);
			}
			opValue.set(valArray);
			output.collect(key, opValue);
		}
	}

	public static JobConf getJobConfig(Configuration config, Class class1, 
			String inPath, String opPath, String cachePath) {
		JobConf conf = new JobConf(config, class1);
		conf.setJobName("IMDB K-final iteration");
		
		 // Create the Hadoop job.
		if(!Constant.DEBUG)
		conf.setInputFormat(SequenceFileInputFormat.class);
		

        // Configure the mapper.
		conf.setMapperClass(FinalKMapper.class);
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(IntWritable.class);

        // Configure the reducer.
        conf.setReducerClass(FinalKReducer.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntArrayWritable.class);
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

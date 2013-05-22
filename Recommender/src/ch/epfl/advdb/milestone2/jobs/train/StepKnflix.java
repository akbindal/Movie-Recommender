package ch.epfl.advdb.milestone2.jobs.train;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import ch.epfl.advdb.milestone2.utils.FloatArrayWritable;
import ch.epfl.advdb.milestone2.utils.Kmath;

public class StepKnflix {
	
	/**
	 *input <<mid>, <FloatArrayWritable>*>
	 *output<centerId, <floatArrayWritable>>
	 *cache: read center
	 * @author ashish
	 *
	 */
	public static class StepKMapper extends MapReduceBase
	implements Mapper<IntWritable, FloatArrayWritable, 
	IntWritable, FloatArrayWritable> {
		
		float[][] centers = new float[Constant.NFLIX_K][10];
		
		boolean[] chkCenter = new boolean[Constant.NFLIX_K];

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
				
			reader = new SequenceFile.Reader(fs, path, conf);
			IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
			FloatArrayWritable value = (FloatArrayWritable) reader.getValueClass().newInstance();
			while(reader.next(key, value))	{
				//System.out.println("key="+key.toString()+"value"+value.toString());
				int centerId = key.get();
				chkCenter[centerId] = true;
				FloatWritable[] vect = (FloatWritable[]) value.toArray();
				for(int i=0; i<10; i++) {
					centers[centerId][i]=vect[i].get();
				}
			}
			
			//generate new center for empty ones
			for(int i =0 ;i<Constant.NFLIX_K;i++) {
				if(!chkCenter[i]) {
					for(int j=0; j<10;j++) {
						float fet = (float) (-1+Kmath.getRandom()*2);
						centers[i][j]=fet;
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
		public void map(IntWritable inKey, FloatArrayWritable inValue,
				OutputCollector<IntWritable, FloatArrayWritable> output, 
				Reporter arg3)
				throws IOException {
			//inkey is movieid
			//invalue is features
			//find the minimum value
			float dist = 0;
			float minDist = 898989;
			int minCenter = -1;
			for(int i=0; i<Constant.NFLIX_K; i++) {
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
		float getDistance(int ci, FloatArrayWritable vect) {
			FloatWritable[] fet = (FloatWritable[]) vect.toArray();
			float distance =0;
			float len1=0, len2=0;
			for(int i =0 ;i<10; i++) {
				float val = centers[ci][i]*fet[i].get();
				distance += (val)*(val);
				len1+=centers[ci][i]*centers[ci][i];
				len2+=fet[i].get()*fet[i].get();
			}
			distance = (float) (distance/(Math.sqrt(len1)*Math.sqrt(len2)));
			return distance;
		}
	}

	static class StepKReducer extends MapReduceBase implements 
	Reducer<IntWritable, FloatArrayWritable, IntWritable, FloatArrayWritable>  {
		
		private FloatArrayWritable opValue = new FloatArrayWritable();

		@Override
		public void reduce(IntWritable key, Iterator<FloatArrayWritable> values,
				OutputCollector<IntWritable, FloatArrayWritable> output, Reporter reporter)
				throws IOException {
			float[] center = new float[10];
			int count =0;
			while(values.hasNext()) {
				FloatWritable[] f = (FloatWritable[]) values.next().toArray();
				for(int i =0; i<10; i++) {
					center[i]+=f[i].get();
				}
				count++;
			}
			if(count>0) {
				for(int i =0; i<10; i++) {
					center[i]=center[i]/count;
				}
			} 
			opValue.setFloatArray(center);
			output.collect(key, opValue);
		}
	}
	
	public static JobConf getJobConfig(Configuration config, Class class1, 
			String inPath, String opPath, String cachePath) {
		JobConf conf = new JobConf(config, class1);
		conf.setJobName("NFLIX Iter :"+opPath);
		
		 // Create the Hadoop job.
		if(!Constant.DEBUG)
		conf.setInputFormat(SequenceFileInputFormat.class);
		
	
	    // Configure the mapper.
		conf.setMapperClass(StepKMapper.class);
	    conf.setMapOutputKeyClass(IntWritable.class);
	    conf.setMapOutputValueClass(FloatArrayWritable.class);
	
	    // Configure the reducer.
	    conf.setReducerClass(StepKReducer.class);
	    conf.setOutputKeyClass(IntWritable.class);
	    conf.setOutputValueClass(FloatArrayWritable.class);
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

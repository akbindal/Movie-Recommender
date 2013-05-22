package ch.epfl.advdb.milestone2.jobs.test;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import ch.epfl.advdb.milestone2.config.Constant;
import ch.epfl.advdb.milestone2.utils.IntArrayWritable;
import ch.epfl.advdb.milestone2.utils.IntFloatPair;
import ch.epfl.advdb.milestone2.utils.IntFloatPairArray;

public class ImdbClusterMapping {
	public static class ClusterMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, IntWritable, IntArrayWritable> {

		ArrayList<Map<Integer, Float>> centers = new ArrayList<Map<Integer,Float>>(Constant.K);

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
				Map<Integer, Float> centerVect = centers.get(centerId);
				IntFloatPair[] ifp = (IntFloatPair[]) value.toArray();
				for(int i=0; i<ifp.length; i++) {
					centerVect.put(ifp[i].getFirst(), ifp[i].getSecond());
				}
			}
			reader.close();
			} catch(Exception e) {
				System.out.println("WARNING: No "+path.toString()+":Couldn't read cache file:"+e.toString());
			}
		}
		IntWritable opKey = new IntWritable();
		IntArrayWritable inValue = new IntArrayWritable();
		@Override
		public void map(LongWritable inKey, Text value,
				OutputCollector<IntWritable, IntArrayWritable> output, 
				Reporter arg3)
				throws IOException {
			String vect = value.toString();
			String[] tokens = vect.split(",");
			if(tokens.length<2) return; //no features
			
			//float length = (float) Math.sqrt(vec.size()); //get the size of vector
			opKey.set(Integer.parseInt(tokens[0]));
			inValue.setStringArray(tokens, 1);
			float[] dist = new float[Constant.K];
			float minDist = 898989;
			for(int i=0; i<Constant.K; i++) {
				dist[i] = getDistance(i, inValue);
				if(dist[i]<minDist) {
					
					minDist = dist[i];
				}
			}
			float threshold = Constant.MAP_THREHSOLD*minDist;
			List<Integer> clusters = new ArrayList<Integer>();
			for(int i=0; i<Constant.K; i++) {
				if(dist[i]<=threshold) {
					clusters.add(i);
				}
			}
			//System.out.println("inkey="+opKey+":centr="+i+"dist=")
			inValue.setIntegerCollection(clusters);
			output.collect(opKey, inValue);
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
	
	public static JobConf getJobConfig(Configuration config, Class class1, 
			String inPath, String opPath, String cachePath) {
		JobConf conf = new JobConf(config, class1);
		conf.setJobName("Map(imdbcluster: netflix cluster)");
		
		 // Create the Hadoop job.
		

        // Configure the mapper.
		conf.setMapperClass(ClusterMapper.class);
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(IntArrayWritable.class);

        // Configure the reducer.
        //conf.setReducerClass(FinalKReducer.class);
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

package ch.epfl.advdb.milestone2.jobs.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import ch.epfl.advdb.milestone2.config.Constant;
import ch.epfl.advdb.milestone2.jobs.test.NflixClusterMapping.ClusterUserMapper;
import ch.epfl.advdb.milestone2.utils.FloatArrayWritable;
import ch.epfl.advdb.milestone2.utils.IntArrayWritable;

/**
 * maps imdb cluster with nflix cluster
 * this is one to many mapping
 * @author ashish
 *
 */
public class ImdbNflixMapping {
	static class ClusterUserMapper extends MapReduceBase implements 
	Mapper<IntWritable, IntArrayWritable, IntWritable, IntArrayWritable>{
		
		Map<Integer, Set<Integer>> nflixCluster = new HashMap<Integer, Set<Integer>>();
		@Override
		public void configure(JobConf job)  {
			
			Path[] cacheFiles;
			try {
				cacheFiles = DistributedCache.getLocalCacheFiles(job);
				if (null != cacheFiles && cacheFiles.length > 0) {
					FileSystem fs = FileSystem.get(job);
			    	for (Path cachePath : cacheFiles) {
			    		System.out.println(cachePath.toString());
			    		loadNflixMapping(fs, cachePath, job);
			        }
			    }
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		}
		
		//read u file 
		public void loadNflixMapping(FileSystem fs, Path  path, Configuration conf){
			SequenceFile.Reader reader ;
			try {
			reader = new SequenceFile.Reader(fs, path, conf);
			IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
			IntArrayWritable value = (IntArrayWritable) reader.getValueClass().newInstance();
			while(reader.next(key, value))	{
				//System.out.println("key="+key.toString()+"value"+value.toString());
				int centerId = key.get();
				IntWritable[] vect = (IntWritable[]) value.toArray();
				Set<Integer> x = new HashSet<Integer>();
				for(int i =0; i<vect.length; i++) {
					x.add(vect[i].get());
				}
				nflixCluster.put(centerId, x);
			}
			reader.close();
			} catch(Exception e) {
				System.out.println("WARNING: No "+path.toString()+":Couldn't read cache file:"+e.toString());
			}
		}
		IntArrayWritable opValue = new IntArrayWritable();
		
		@Override
		public void map(IntWritable key, IntArrayWritable value,
				OutputCollector<IntWritable, IntArrayWritable> output, Reporter reporter)
				throws IOException {
			IntWritable[] xMovie = (IntWritable[]) value.toArray();
			Set<Integer> imdbMovie = new HashSet<Integer>();
			for(int i =0 ;i<xMovie.length; i++) {
				imdbMovie.add(xMovie[i].get());
			}
			Map<Integer, Float> score = new HashMap<Integer, Float>();
			float minScore = Float.MAX_VALUE;
			for(Entry<Integer, Set<Integer>> e: nflixCluster.entrySet()){
				Set<Integer> intersection = new HashSet<Integer>(imdbMovie);
				Set<Integer> union = new HashSet<Integer> (imdbMovie);
				intersection.retainAll(e.getValue());
				float interSize = intersection.size();
				union.addAll(e.getValue());
				float unionSize = union.size();
				float dist= Float.MAX_VALUE;
				if(unionSize>0)
					dist = 1-(interSize/unionSize);
				if(dist<minScore) {
					minScore = dist;
				}
				score.put(e.getKey(), dist);
			}
			List<Integer> out = new ArrayList<Integer>();
			float threshold = Constant.CLUSTER_MAP_THRESHOLD*minScore;
			for(Entry<Integer, Float> e: score.entrySet()) {
				if(e.getValue()<=threshold) 
					out.add(new Integer(e.getKey()));
			}
			opValue.setIntegerCollection(out);
			output.collect(key, opValue);
		}
	}
	
	public static JobConf getJobConfig(Configuration config, Class class1, 
			String inPath, String opPath, String cachePath) {
		JobConf conf = new JobConf(config, class1);
		conf.setJobName("Map(imdb cluster and nflix cluster)");
		
		//configure mapper
		if(!Constant.DEBUG)
			conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(ClusterUserMapper.class);
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
		
		//conf.set("mapred.textoutputformat.separator","\t");		
		
		FileInputFormat.addInputPath(conf, new Path(inPath));
		FileOutputFormat.setOutputPath(conf, new Path(opPath));

		conf.setNumReduceTasks(0);
		
		//Delete the output.
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			fs.delete(new Path(opPath), true);
			FileStatus[] filestatus = fs.listStatus(new Path(cachePath));
			for (FileStatus status : filestatus) {
			    DistributedCache.addFileToClassPath(status.getPath(), conf);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conf;
	}
}

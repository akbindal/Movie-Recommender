package ch.epfl.advdb.milestone2.jobs.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

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
import ch.epfl.advdb.milestone2.jobs.test.ImdbNflixMapping.ClusterUserMapper;
import ch.epfl.advdb.milestone2.utils.ImdbVector;
import ch.epfl.advdb.milestone2.utils.IntArrayWritable;
import ch.epfl.advdb.milestone2.utils.IntFloatPairArray;

public class ImdbUserMapping {
	
	static class ImdbMapper extends MapReduceBase implements 
	Mapper<IntWritable, IntArrayWritable, IntWritable, IntArrayWritable>{
		
		Map<Integer, Set<Integer>> nflixToImdb = new HashMap<Integer, Set<Integer>>();
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
				int imdbCluster = key.get();
				IntWritable[] nflixClusters  = (IntWritable[]) value.toArray();
				for(int i =0; i<nflixClusters.length; i++) {
					int nflixC = nflixClusters[i].get();
					if(nflixToImdb.containsKey(nflixC)) {
						nflixToImdb.get(nflixC).add(imdbCluster);
					} else {
						Set<Integer> x = new HashSet<Integer>();
						x.add(imdbCluster);
						nflixToImdb.put(nflixC, x);
					}
				}
			}
			reader.close();
			} catch(Exception e) {
				System.out.println("WARNING: No "+path.toString()+":Couldn't read cache file:"+e.toString());
			}
		}
		IntWritable opKey = new IntWritable();
		/**
		 * 
		 * @param key = nflix movie 
		 * @param value = users (arrayWritable)
		 * @param output = imdb movie from cache with users
		 * @param reporter
		 * @throws IOException
		 */
		@Override
		public void map(IntWritable key, IntArrayWritable value,
				OutputCollector<IntWritable, IntArrayWritable> output, Reporter reporter)
				throws IOException {
			Set<Integer> imdbMovies = nflixToImdb.get(key.get());
			if(imdbMovies==null) return;
			for(Integer m: imdbMovies) {
				opKey.set(m);
				output.collect(opKey, value);
			}
		}
	}
	
	static class ImdbReducer extends MapReduceBase implements 
	Reducer<IntWritable, IntArrayWritable, IntWritable, IntArrayWritable>  {
		
		private IntArrayWritable opValue = new IntArrayWritable();
		private Set<IntWritable> userList = new HashSet<IntWritable>();
		
		@Override
		public void reduce(IntWritable key, Iterator<IntArrayWritable> values,
				OutputCollector<IntWritable, IntArrayWritable> output, Reporter reporter)
				throws IOException {
			userList.clear();
			while(values.hasNext()) {
				IntWritable[] x = (IntWritable[]) values.next().toArray();
				for(int i =0; i< x.length; i++) {
					userList.add(x[i]);
				}
			}
			IntWritable[] users = new IntWritable[userList.size()];
			int ind = 0;
			for(IntWritable u: userList) {
				users[ind]=u;
				ind++;
			}
			opValue.set(users);
			output.collect(key, opValue);
		}
	}
	
	public static JobConf getJobConfig(Configuration config, Class class1, 
			String inPath, String opPath, String cachePath) {
		JobConf conf = new JobConf(config, class1);
		conf.setJobName(" Map(imdb cluster and user)");
		
		//configure mapper
		if(!Constant.DEBUG)
			conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(ImdbMapper.class);
		conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(IntArrayWritable.class);
        
        //confgiure the reducer
        /** no need identitiy reducer**/
		conf.setReducerClass(ImdbReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntArrayWritable.class);
		conf.setNumReduceTasks(Constant.NO_REDUCER);
		
		if(!Constant.DEBUG) {
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setCompressOutput(conf, true);
			SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.RECORD);
		}
		
		//conf.set("mapred.textoutputformat.separator","\t");		
		
		FileInputFormat.addInputPath(conf, new Path(inPath));
		FileOutputFormat.setOutputPath(conf, new Path(opPath));

		//conf.setNumReduceTasks(0);
		
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

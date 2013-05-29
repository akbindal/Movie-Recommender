package ch.epfl.advdb.milestone2.jobs.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import ch.epfl.advdb.milestone2.config.Constant;
import ch.epfl.advdb.milestone2.jobs.test.ImdbUserMapping.ImdbMapper;
import ch.epfl.advdb.milestone2.jobs.test.ImdbUserMapping.ImdbReducer;
import ch.epfl.advdb.milestone2.utils.IntArrayWritable;

/**
 * map each test movie to user 
 * cache: mapping from test movie to imdb cluster
 * input: imdb cluster to user mapping
 * output: mapping from test movie to user
 * @author ashish
 *
 */
public class RecommendUser {
	
	static class MovieUserMapper extends MapReduceBase implements 
	Mapper<IntWritable, IntArrayWritable, IntWritable, IntArrayWritable>{
		
		Map<Integer, Set<Integer>> imClusterToMovie = new HashMap<Integer, Set<Integer>>();
		
		@Override
		public void configure(JobConf job)  {
			
			Path[] cacheFiles;
			try {
				cacheFiles = DistributedCache.getLocalCacheFiles(job);
				if (null != cacheFiles && cacheFiles.length > 0) {
					FileSystem fs = FileSystem.get(job);
			    	for (Path cachePath : cacheFiles) {
			    		System.out.println(cachePath.toString());
			    		loadTestMovieMapping(fs, cachePath, job);
			        }
			    }
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		}
		
		//read u file 
		public void loadTestMovieMapping(FileSystem fs, Path  path, Configuration conf){
			SequenceFile.Reader reader ;
			try {
			reader = new SequenceFile.Reader(fs, path, conf);
			IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
			IntArrayWritable value = (IntArrayWritable) reader.getValueClass().newInstance();
			while(reader.next(key, value))	{
				//System.out.println("key="+key.toString()+"value"+value.toString());
				int testMovie = key.get();
				IntWritable[] imdbClusters  = (IntWritable[]) value.toArray();
				for(int i =0; i<imdbClusters.length; i++) {
					int imdbC = imdbClusters[i].get();
					if(imClusterToMovie.containsKey(imdbC)) {
						imClusterToMovie.get(imdbC).add(testMovie);
					} else {
						Set<Integer> x = new HashSet<Integer>();
						x.add(testMovie);
						imClusterToMovie.put(imdbC, x);
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
		public void map(IntWritable key, IntArrayWritable value,
				OutputCollector<IntWritable, IntArrayWritable> output, Reporter reporter)
				throws IOException {
			Set<Integer> testMovies = imClusterToMovie.get(key.get());
			if(testMovies==null) return;
			for(Integer m: testMovies) {
				opKey.set(m);
				output.collect(opKey, value);
			}
		}
	}
	
	static class MovieUserReducer extends MapReduceBase implements 
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
		conf.setJobName("Map(TestMovie and user)");
		
		//configure mapper
		if(!Constant.DEBUG)
			conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(MovieUserMapper.class);
		conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(IntArrayWritable.class);
        
        //confgiure the reducer
        /** no need identitiy reducer**/
		conf.setReducerClass(MovieUserReducer.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntArrayWritable.class);
		conf.setNumReduceTasks(Constant.NO_REDUCER);
		if(!Constant.DEBUG) {
//			conf.setOutputFormat(SequenceFileOutputFormat.class);
//			SequenceFileOutputFormat.setCompressOutput(conf, true);
//			SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.RECORD);
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

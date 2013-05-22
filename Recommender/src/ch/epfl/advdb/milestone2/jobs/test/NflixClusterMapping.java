package ch.epfl.advdb.milestone2.jobs.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
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
import ch.epfl.advdb.milestone2.utils.FloatArrayWritable;
import ch.epfl.advdb.milestone2.utils.IntArrayWritable;

/**
 * map each nflix cluster with users
 * cache: U matrix
 * input: cluster centers from previous iter
 * output: clusterId: and UserList
 * @author ashish
 *
 */
public class NflixClusterMapping {
	
	static class ClusterUserMapper extends MapReduceBase implements 
	Mapper<IntWritable, FloatArrayWritable, IntWritable, IntArrayWritable>{
		
		float[][] uFeature = new float[Constant.NO_USER+1][10];

		@Override
		public void configure(JobConf job)  {
			
			Path[] cacheFiles;
			try {
				cacheFiles = DistributedCache.getLocalCacheFiles(job);
				if (null != cacheFiles && cacheFiles.length > 0) {
			    	for (Path cachePath : cacheFiles) {
			    		System.out.println(cachePath.toString());
			    		loadUMatrix(cachePath);
			        }
			    }
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		}
		
		//read u file 
		public void loadUMatrix(Path  path){
			BufferedReader fileReader=null;
			try {
				fileReader= new BufferedReader(
				        new FileReader(path.toString()));
				String line;
				while ((line = fileReader.readLine()) != null) {
					String[] tokens = line.split(",");
					if(!tokens[0].equals("U")) continue;
					int userid = Integer.parseInt(tokens[1]);
					String featureIndex = tokens[2]; // = tokens[1].split(",");
					
					int fi = Integer.parseInt(featureIndex);
					String featureValue = tokens[3];
					float fv = Float.parseFloat(featureValue);
					uFeature[userid][fi-1]=fv;
					
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally {
			      try {
					fileReader.close();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		}
		IntArrayWritable opValue = new IntArrayWritable();
		
		@Override
		public void map(IntWritable key, FloatArrayWritable value,
				OutputCollector<IntWritable, IntArrayWritable> output, Reporter reporter)
				throws IOException {
			
			List<Integer> userList = new ArrayList<Integer>();
			FloatWritable[] curVect = (FloatWritable[]) value.toArray();
			float mul=0;
			for(int i=1; i<Constant.NO_USER; i++) {
				mul=0;
				for(int j=0; j<10; j++) {
					mul+=curVect[j].get()*uFeature[i][j];
				}
				if(mul>Constant.USER_FILTER) {
					userList.add(i);
				}
			}
			
			opValue.setIntegerCollection(userList);
			output.collect(key, opValue);
		}


		
	}
	
	public static JobConf getJobConfig(Configuration config, Class class1, 
			String inPath, String opPath, String cachePath) {
		JobConf conf = new JobConf(config, class1);
		conf.setJobName("nflix Map(cluster and user)");
		
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

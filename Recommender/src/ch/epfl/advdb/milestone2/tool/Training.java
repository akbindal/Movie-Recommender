package ch.epfl.advdb.milestone2.tool;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;

import ch.epfl.advdb.milestone2.config.Constant;
import ch.epfl.advdb.milestone2.config.FileConfig;
import ch.epfl.advdb.milestone2.jobs.test.ImdbClusterMapping;
import ch.epfl.advdb.milestone2.jobs.test.ImdbNflixMapping;
import ch.epfl.advdb.milestone2.jobs.test.ImdbUserMapping;
import ch.epfl.advdb.milestone2.jobs.test.NflixClusterMapping;
import ch.epfl.advdb.milestone2.jobs.test.RecommendUser;
import ch.epfl.advdb.milestone2.jobs.train.FinalKImdb;
import ch.epfl.advdb.milestone2.jobs.train.FinalKNflix;
import ch.epfl.advdb.milestone2.jobs.train.InitKimdb;
import ch.epfl.advdb.milestone2.jobs.train.InitKnflix;
import ch.epfl.advdb.milestone2.jobs.train.ParseImdbData;
import ch.epfl.advdb.milestone2.jobs.train.ParseNflixData;
import ch.epfl.advdb.milestone2.jobs.train.StepKimdb;
import ch.epfl.advdb.milestone2.jobs.train.StepKnflix;

public class Training extends Configured implements Tool{
	
	
	private static String stepKIMDB = Constant.stepKIMDB;
	private static String stepKNFLIX = Constant.stepKNFLIX;
	
	/*
	 * All the job sequences for the UV decomposition algorithm
	 * is written. 
	 * In order to run the jobs, This function use two methods which are defined in mapred api
	 * 1. JobClient runs the single job.
	 * 2. JobControl best to run the sequence of dependent jobs in easier way, but this is bit 
	 * slower than JobClient for running sequential job (slower in terms of setting up the jobs)
	 * 
	 * for iteration, Jobs are executed through JobClient. 
	 * otherwise all the jobs are run through jobControl
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		
		String inputPath=null;
		String tempPath = null;
		String cachePath=null;
		
		/******************TRAINING***/
		JobControl jc = new JobControl("Training");
		
		/** converting imdb data to sequential file**/
		inputPath = FileConfig.TRAIN_DATASET+
				"features";
		tempPath = FileConfig.TEMP_PATH+
				"feature_seq";
		
		JobConf parseImdbData = ParseImdbData.getJobConfig(getConf(), getClass(), 
				inputPath, tempPath);
		Job jparseImdb = new Job(parseImdbData);
		jc.addJob(jparseImdb);
		//JobClient.runJob(parseImdbData);
		/** converting nflix data to sequential file**/
		inputPath = FileConfig.TRAIN_DATASET+
				"V";
		tempPath = FileConfig.TEMP_PATH+
				"V_seq";

		JobConf parseNflixData = ParseNflixData.getJobConfig(getConf(), getClass(), 
				inputPath, tempPath);
		Job jparseNflix = new Job(parseNflixData);
		jc.addJob(jparseNflix);
		
		
		
		/**IMDB clustering:Kmean***/
		
		/**Imdb cluster initialization**/
		inputPath = FileConfig.TEMP_PATH+
				"feature_seq";
		tempPath = FileConfig.TEMP_PATH+
				stepKIMDB+"0";
		JobConf cInitKimdb = InitKimdb.getJobConfig(getConf(), getClass(), 
				inputPath, tempPath);
		Job jInitKimdb = new Job(cInitKimdb);
		
		jInitKimdb.addDependingJob(jparseImdb);
		jc.addJob(jInitKimdb);
//		JobClient.runJob(cInitKimdb);
		
		/**neflix cluster initialization**/
		inputPath = FileConfig.TEMP_PATH+
				"V_seq";
		tempPath = FileConfig.TEMP_PATH+
				stepKNFLIX+"0";
		JobConf cInitKnflix = InitKnflix.getJobConfig(getConf(), getClass(), 
				inputPath, tempPath);
		Job jInitKnflix = new Job(cInitKnflix);
		jInitKnflix.addDependingJob(jparseNflix);
		jc.addJob(jInitKnflix);
		//JobClient.runJob(cInitKnflix);
		
				
		
		System.out.println("running init k nflix");
		
		/***RUN THE JOB CLIENT****/
		/** start the Job Execution and wait till it is over**/
		Thread runjobc = new Thread(jc);
		runjobc.start();
		while (!jc.allFinished()) {
			System.out.println("\n******Waiting JOBS*****");
			System.out.println("left Jobs="+jc.getWaitingJobs().toString());
			System.out.println("\n******RUNNING JOBS*****");
			System.out.println("left Jobs="+jc.getWaitingJobs().toString());
			Thread.sleep(25*1000);
		}
		
		/**imdb and netflix step**/
		int iter = 1;
		while(iter <Constant.MAX_ITERATION) {
			System.out.println("Kmean Iter="+iter);
			
			/***  imdb cluster step ***/
			inputPath = FileConfig.TEMP_PATH+
					"feature_seq";
			tempPath = FileConfig.TEMP_PATH+stepKIMDB + iter;
			cachePath = FileConfig.TEMP_PATH+stepKIMDB+(iter-1);
			
			JobConf jcstepKimdb = StepKimdb.getJobConfig(getConf(), 
					getClass(), inputPath, tempPath, cachePath);
			
	        JobClient.runJob(jcstepKimdb);
	        
	        
			/*** nflix cluster step ***/
	        inputPath = FileConfig.TEMP_PATH+
					"V_seq";
			tempPath = FileConfig.TEMP_PATH+stepKNFLIX + iter;
			cachePath = FileConfig.TEMP_PATH+stepKNFLIX+(iter-1);
			JobConf jcstepKnflix = StepKnflix.getJobConfig(getConf(), getClass(),
					inputPath, tempPath, cachePath);
			JobClient.runJob(jcstepKnflix);
	      
	        //delete data from previous step clusters
			FileSystem fs;        
	        try {
				fs = FileSystem.get(getConf());
				//Delete the output.
				fs.delete(new Path(FileConfig.TEMP_PATH+stepKIMDB+(iter-1)), true);
				fs.delete(new Path(FileConfig.TEMP_PATH+stepKNFLIX+(iter-1)), true);
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
			iter++;
		}
		
		//Following output stores the mapping of the movie with clusters
		//call final kimd iteration 
		inputPath = FileConfig.TEMP_PATH+
				"feature_seq";
		tempPath = FileConfig.TEMP_PATH+stepKIMDB + iter;
		cachePath = FileConfig.TEMP_PATH+stepKIMDB+(iter-1);

		JobConf jcstepKimdb = FinalKImdb.getJobConfig(getConf(), 
				getClass(), inputPath, tempPath, cachePath);
        JobClient.runJob(jcstepKimdb);
        
        //call final nflix K iteration for mapping movies with cluster
        inputPath = FileConfig.TEMP_PATH+
				"V_seq";
		tempPath = FileConfig.TEMP_PATH+stepKNFLIX + iter;
		cachePath = FileConfig.TEMP_PATH+stepKNFLIX+(iter-1);

		JobConf jcstepKnflix = FinalKNflix.getJobConfig(getConf(), 
				getClass(), inputPath, tempPath, cachePath);
        JobClient.runJob(jcstepKnflix);
        
        return 0;
	}

}

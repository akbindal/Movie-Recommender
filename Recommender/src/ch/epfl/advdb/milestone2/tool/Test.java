package ch.epfl.advdb.milestone2.tool;

import org.apache.hadoop.conf.Configured;
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

public class Test extends Configured implements Tool{
	
	private static String stepKIMDB = Constant.stepKIMDB;
	private static String stepKNFLIX = Constant.stepKNFLIX;
	
	@Override
	public int run(String[] arg0) throws Exception {
		String inputPath=null;
		String tempPath = null;
		String cachePath= null;
		int iter = Constant.MAX_ITERATION;
		
		/***JObs for achieving Mapping from imdb cluster to user **/
		JobControl jc = new JobControl("mapping and testing");

		/************mapping nflix cluster with users****************/
        inputPath = FileConfig.TEMP_PATH+stepKNFLIX+(iter-1);
		cachePath = FileConfig.TRAIN_DATASET+
				"U";
		tempPath = FileConfig.TEMP_PATH+"NFlixUserMap";
		
		JobConf jcMapNflix = NflixClusterMapping.getJobConfig(getConf(), 
				getClass(), inputPath, tempPath, cachePath);
        //JobClient.runJob(jcMapNflix);
        Job jMapNflix = new Job(jcMapNflix);
		jc.addJob(jMapNflix);
        
        /*******Mapping imdb cluster with nflix cluster*****/
        inputPath = FileConfig.TEMP_PATH+stepKIMDB + iter;
		cachePath = FileConfig.TEMP_PATH+stepKNFLIX + iter;
		tempPath = FileConfig.TEMP_PATH+"ClusterMap";
		
		JobConf jcMapImdbNflix = ImdbNflixMapping.getJobConfig(getConf(), 
				getClass(), inputPath, tempPath, cachePath);
        //JobClient.runJob(jcMapImdbNflix);
        Job jMapImdbNflix = new Job(jcMapImdbNflix);
		jc.addJob(jMapImdbNflix);
		
		Thread runjobc = new Thread(jc);
		runjobc.start();
		while (!jc.allFinished()) {
			System.out.println("\n******Waiting JOBS*****");
			System.out.println("left Jobs="+jc.getWaitingJobs().toString());
			System.out.println("\n******RUNNING JOBS*****");
			System.out.println("left Jobs="+jc.getWaitingJobs().toString());
			Thread.sleep(1000*25);
		}		
		
        /****final mapping for imdb cluster with user:combine result from above***/
		//DependingJob(jcMapImdbNflix);
        //DependingJob(jcMapNflix);
		inputPath = FileConfig.TEMP_PATH+"NFlixUserMap";
		cachePath =  FileConfig.TEMP_PATH+"ClusterMap";
		tempPath = FileConfig.TEMP_PATH+"movieUser";
		
		JobConf jcMapImdbUser = ImdbUserMapping.getJobConfig(getConf(), 
				getClass(), inputPath, tempPath, cachePath);
        JobClient.runJob(jcMapImdbUser);
        
        
        /***Map testing movies with imdb cluster**/
        inputPath = FileConfig.TEST_DATASET+
				"features";
		cachePath = FileConfig.TEMP_PATH+stepKIMDB+(iter-1);
		tempPath = FileConfig.TEMP_PATH+"testimdbmap";
		
		JobConf jcMapImdb = ImdbClusterMapping.getJobConfig(getConf(), 
				getClass(), inputPath, tempPath, cachePath);
		JobClient.runJob(jcMapImdb);
		
		/** Final Recommendation***/
		//DependingJob(jcMapImdb);
        //DependingJob(jcMapImdbUser);
		inputPath = FileConfig.TEMP_PATH+"movieUser";
		cachePath =  FileConfig.TEMP_PATH+"testimdbmap";
		tempPath = FileConfig.OUTPUT_PATH;
		
		JobConf jcRecommendUser = RecommendUser.getJobConfig(getConf(), 
				getClass(), inputPath, tempPath, cachePath);
        JobClient.runJob(jcRecommendUser);
		return 0;
	}

}

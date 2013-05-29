package ch.epfl.advdb.milestone2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import ch.epfl.advdb.milestone2.config.FileConfig;
import ch.epfl.advdb.milestone2.tool.Test;
import ch.epfl.advdb.milestone2.tool.Training;



public class Main {
public static boolean CALCULATE_RMSE=false;
	
	
	/**
	 * Ip/Op path for submitted code:
	 * Training Set: /netflix_imdb/input/train 
	 * Test Set: /netflix_imdb/input/test 
	 * Output: /stdX/output
	 */
	
	
	/**
	 * fscore in development mode only
	 * 
	 */
	
	
	public static void main(String[] args) throws Exception {
		if (args.length < 0) {
			System.err
					.println("Usage: ch.epfl.advdb.milestone2 <input path> <output path>");
			System.exit(-1);
		}
		pathSetup(args[0], args[1], args[2]);
		//Training Tool
		int res = ToolRunner.run(new Configuration(), new Training(), args);
		//System.out.println(res);
		
		res = ToolRunner.run(new Configuration(), new Test(), args);
		System.out.println("COMPLETE");
		System.exit(res);
	}
	
	public static void pathSetup(String inTrain, String inTest, String output) {
		FileConfig.TRAIN_DATASET=inTrain+"/";
		FileConfig.TEST_DATASET = inTest+"/";
		FileConfig.OUTPUT_PATH = output+"/";
		FileConfig.TEMP_PATH=FileConfig.OUTPUT_PATH+"../temp/";
	}
	
}

package ch.epfl.advdb.milestone2.config;

public class Constant {
	public static final int FEATURES = 1852;
	public static final float MAP_THREHSOLD = (float) 1.05; //used for test movie with imdb cluster
	public static final float CLUSTER_MAP_THRESHOLD = (float) 1.05; //use for mapping imdb cluster with nflix cluster
	public static final float USER_FILTER = (float)0.2; //used for filtering user based on rating>0.5
	
	public static int NFLIX_K=100; //number of clustring for NFLIX clustering
	public static final int NO_USER = 478867;
	public static int K=80;//number of cluster for IMDB clustering
	public static int MAX_ITERATION=10;
	public static float K_INITIALIZATION = (float)0.2; //used for prob of selecting vector
	public static boolean DEBUG=false;
	public static String stepKIMDB = "imdb_centers_";
	public static String stepKNFLIX = "nflix_centers_";
	public static final int NO_REDUCER = 44;
}

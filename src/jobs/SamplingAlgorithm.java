package jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import readers.GraphReader;
import scala.Tuple2;
import vos.Graph;
import vos.MMS;

public class SamplingAlgorithm {

	public final static Random random = new Random();

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception  {

		SparkConf sparkConf = new SparkConf().setAppName("OutlierPrune").setMaster("local[2]").set("spark.executor.memory","4g");
		sparkConf.set("com.wildfire.graph_file_path", args[0]);
		sparkConf.set("com.wildfire.mms_file_path", args[1] + "/mms.csv");
		sparkConf.set("com.wildfire.removal_nodes_file_path", args[1] + "/removal-nodes.csv");

		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		System.out.println("Leyendo grafo");

		// First, we read the graph and get the original MMS
		Graph graph = GraphReader.read(ctx);

		System.out.println(graph.getNeighbours());

		System.out.println("Calculando MMS inicial");
		MMS mms = graph.clone().getMMS();


		// We save the number of configurations required for the algorithm to converge
		int n = graph.getNumberOfNodes();
		double configurationsNumber = Math.ceil(n*Math.log(n));
		sparkConf.set("com.wildfire.configurations_number", String.valueOf(configurationsNumber));

		ArrayList<String[]> driverNodesSets = new ArrayList<String[]>();

		//Then we perform the iterative algorithm
		for(int i = 0; i < configurationsNumber; i++) {
			System.out.print("Calculando MMS " + i + " de " + configurationsNumber + ": ");

			// Now, we get the matched nodes and the node to be removed form this MMS
			ArrayList<String> matchedNodes = new ArrayList<String>(mms.getMatchedNodes());
			String nodeToRemove = matchedNodes.get(random.nextInt(matchedNodes.size()));


			// We prepare the array where we are going to save the configurations of unmatched nodes
			ArrayList<MMS> alternativesMMS = new ArrayList<MMS>();

			// While we can remove nodes
			while(mms.removeNode(nodeToRemove)) {
				System.out.print("*");
				// We store a clone and remove the graph as it's not quite useful and memory is expensive these days 
				MMS nextmms = mms.clone();
				nextmms.setGraph(null);
				// We store the unmatched nodes as a driver node configuration
				alternativesMMS.add(nextmms);
				// And we get the matched nodes to obtain the next random removal node
				matchedNodes = new ArrayList<String>(mms.getMatchedNodes());
				nodeToRemove = matchedNodes.get(random.nextInt(matchedNodes.size()));
			}

			System.out.print(nodeToRemove + " -> ");
			System.out.print(mms.getEdges());
			System.out.println();

			// Now we get a random alternative configuration (if there's any)
			if(!alternativesMMS.isEmpty()) {
				mms = alternativesMMS.get(random.nextInt(alternativesMMS.size()));
				// and store the unmatched nodes (a.k.a as driver nodes)
				driverNodesSets.add(mms.getUnmatchedNodes().toArray(new String[mms.getUnmatchedNodes().size()]));
			}

			// We reload the graph so we recover all the removed edges
			mms.setGraph(graph.clone());

		}

		final double alternativeMMSNumber = driverNodesSets.size();


		System.out.println("Conseguidas " + alternativeMMSNumber + " MMS ");

		// At this point we have all the sampled driver node sets and we are ready to count in how many MMSs is each node

		// Now we insert the collection in Spark and flat the collection of iterables
		JavaRDD<String> driverNodes = ctx.parallelize(driverNodesSets).flatMap(new FlatMapFunction<String[], String>() {

			@Override
			public Iterable<String> call(String[] driverNodes)
					throws Exception {
				return  Arrays.asList(driverNodes);
			}

		});

		// Now we should have a list such as A, B, C, A, D, C, F, B, C ...

		// Next we count all the nodes with a basic map/reduce functions for counting
		JavaPairRDD<String, Integer> driverCounts = driverNodes.mapToPair(new PairFunction<String, String, Integer>() {
			// The map function, that emits a 1 for each node
			  public Tuple2<String, Integer> call(String node) { return new Tuple2<String, Integer>(node, 1); }
			}).reduceByKey(new Function2<Integer, Integer, Integer>() {
				// The reduce function that sums all the ones
				public Integer call(Integer a, Integer b) { return a + b; }
		});

		// Now we should have a list such as (A,2) (B,2) (C,3) (D,1) (F,1) ...

		// We now divide all the counting by the number of configurations
		JavaPairRDD<String, Double> controllability = driverCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Double>() {
			  public Tuple2<String, Double> call(Tuple2<String, Integer> node) { return new Tuple2<String, Double>(node._1, node._2/alternativeMMSNumber); }
			});

		controllability.saveAsTextFile(args[2]);

	}

}

import Heet_Sheth_Betweenness.{calculateBetweeness, makeGraph}

import java.io.{File, PrintWriter}

import scala.collection.Map
import scala.util.control.Breaks._

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Heet_Sheth_Community {

  println("In communities object. ")
  val conf = new SparkConf().setAppName("Heet_Sheth_HW5_community").setMaster("local")
  val sc = SparkContext.getOrCreate(conf)
  sc.setLogLevel("WARN")

  def makeCommunities(graph_c: Graph[Int, Int], betweenness_c: RDD[(VertexId, VertexId, Double)], community_output: String): Unit ={

    var graph = graph_c
    val new_A = sc.broadcast(graph.edges.map(edge => ((edge.srcId, edge.dstId), 1)).collectAsMap())
    var communities = graph.connectedComponents().vertices.map(comm => (comm._2, Set(comm._1))).reduceByKey(_ ++ _)

    // Final communities stored in Final_com
    var final_comm = communities.collect()
    val betweenness = betweenness_c.sortBy(x => x._3, ascending = false).map(x => (x._1, x._2)).collect()

    var removed = Set[(Long, Long)]()
    var final_a = Map[(Long, Long), Int]()

    var oldModularity = 0.0
    val removeEdgesInIteration = 1000
    var iterations = 0

    breakable{
      while (iterations < 1500) {
        removed = betweenness.splitAt(iterations * removeEdgesInIteration)._1.toSet

        graph = Graph(graph.vertices, graph.edges.filter(x => (!removed.contains((x.srcId, x.dstId)) && !removed.contains((x.dstId, x.srcId)))))
        val deg = sc.broadcast(graph.degrees.collectAsMap())

        // Check for the # of communities
        var components = graph.connectedComponents()
        var m = components.edges.count()
        communities = components.vertices.map(comm => (comm._2, Set(comm._1))).reduceByKey(_ ++ _)

        // Edges present after removing high betweenness edges
        final_a = new_A.value -- removed
        val final_a_broad = sc.broadcast(final_a)

        // Work on total modularity of graph with multiple communities
        val modRDD = communities.collect().map { case (_, comm_members) =>
          val comm_rdd = sc.parallelize(comm_members.toSeq).cache()
          val comm_pairs = comm_rdd.cartesian(comm_rdd).filter(x => (x._1 < x._2))
          comm_rdd.unpersist()

          comm_pairs.map{ case (i, j) =>
            val aij = Math.max(final_a_broad.value.getOrElse((i.toInt, j.toInt), 0), final_a_broad.value.getOrElse((j.toInt, i.toInt), 0))
            val ki = deg.value(i)
            val kj = deg.value(j)
            aij - ((ki * kj) / (2.0 * m))
          }.sum
        }
        final_a_broad.unpersist()

        val modularity = modRDD.sum / (2.0 * m)
        //println("iteration: " + iterations + " modularity: " + modularity + " communities: " + communities.count())
        if (oldModularity > modularity) {
          break
        }
        final_comm = communities.collect()
        oldModularity = modularity

        iterations += 1
      }
    }

    println("Communities: " + communities.count() + "\n\n")
    // Sort the communities list
    var comm = List[List[VertexId]]()
    for (each <- final_comm) { comm = comm :+ each._2.toList.sorted }
    comm = comm.sortBy(x => (x.head))

    // Print the communities in the file
    val writer = new PrintWriter(new File(community_output))
    for (each <- comm) { writer.write("[" + each.mkString(",") + "]\n") }
    writer.close()
  }


  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    //println("start: " + start)

    // Args
    val input_file = args(0)//"dataset/ratings.csv"//
    val community_output = args(1)//"dataset/Heet_Sheth_communities.txt"//
    val betweenness_output = args(2)//"dataset/Heet_Sheth_betweenness.txt"//

    // Hyper-params
    val threshold = 3 // 3 commonly rated movies

    // makeGraph from commonly rated movie from ratings.csv file
    var (graph_with_ids, id_map) = makeGraph(sc, threshold, input_file)
    println("Done making graph from ratings.csv file")

    // Calculate betweenness
    var (graph, betweenness) = calculateBetweeness(sc, graph_with_ids, id_map, betweenness_output)
    println("Done calculating betweenness!")

    // Make communities through modularity
    makeCommunities(graph, betweenness, community_output)
    println("Done making perfect communities using Girvan-Newman algorithm!")

    var end = System.currentTimeMillis()
    //println(end)
    println("The total execution time taken is " + ((end-start)/1000.0).toString + " sec.")

  }

}
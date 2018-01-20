import java.io.{File, PrintWriter}

import scala.collection.mutable.{HashMap, Queue, Stack}
import scala.collection.Map

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Heet_Sheth_Betweenness {

  def makeGraph(sc: SparkContext, threshold: Int, input_file: String): (Graph[Int, Int], RDD[(VertexId, Int)]) = {

    val vertex_table = sc.textFile(input_file).filter(r => !r.contains("userId"))
      .map{_.split(",") match { case
        Array(user, movie, _, _) => (user.toInt, Set(movie.toInt))
      }}.reduceByKey(_++_).zipWithIndex().map{ case (k,v) => (v,k)}.cache()

    val vertex_table_without_set: RDD[(VertexId, Int)] = vertex_table.map{ case (id, (user, _)) => (id, user)}.cache()
    val default_user = -1

    val edge_rdd = vertex_table.cartesian(vertex_table)
      .filter(pair => pair._1._1 < pair._2._1 && pair._1._2._2.intersect(pair._2._2._2).size >= threshold)
      .map(pair => Edge(pair._1._1, pair._2._1, 1))

    (Graph(vertex_table_without_set, edge_rdd, default_user), vertex_table_without_set)
  }


  def calculateBetweeness(sc: SparkContext, graph: Graph[Int, Int], id_map: RDD[(VertexId, Int)], output_location: String): (Graph[Int, Int], RDD[(VertexId, VertexId, Double)]) = {

    val bcastNeighbors = sc.broadcast(graph.collectNeighborIds(EdgeDirection.Either).collectAsMap())

    val betweenness = graph.vertices.map{ case (root, _) =>
      var q: Queue[VertexId] = new Queue[VertexId]()
      var s: Stack[VertexId] = new Stack[VertexId]()
      var metrics: HashMap[VertexId, (Array[VertexId], Int, Int, Double)] = new HashMap[VertexId, (Array[VertexId], Int, Int, Double)]()
      var neighbors = bcastNeighbors.value

      val num_nodes = neighbors.size
      val betweenness = new HashMap[(VertexId, VertexId), Double]()

      for (node <- 0 until num_nodes) { metrics(node) = (Array[VertexId](), -1, 0, 0.0) }
      metrics(root) = (metrics(root)._1, 0, 1, metrics(root)._4)
      q.enqueue(root)

      while (q.nonEmpty) {
        val v = q.dequeue()

        val (_, vDist,vShort, _) = metrics(v)
        val localNeighbors = neighbors.getOrElse(v, Array())
        s.push(v)

        localNeighbors.foreach ({ edge =>
          val w = edge
          var (wPred, wDist,wShort, wDep) = metrics(w)
          if (wDist == -1) {
            wDist = vDist + 1
            metrics(w) = (wPred, wDist, wShort, wDep)
            q.enqueue(w)
          }
          if (wDist == vDist + 1) {
            wPred = wPred.union(Array(v))
            metrics(w) = (wPred, wDist, wShort + vShort, wDep)
          }
        })
      }

      while (s.nonEmpty) {
        val w = s.pop()
        for (node <- metrics(w)._1) {
          val c = metrics(node)._3.toDouble / metrics(w)._3.toDouble * (1 + metrics(w)._4)
          betweenness((node, w)) = betweenness.getOrElse((node, w), 0.0) + c
          val (pr, di, sh, dep) = metrics(node)
          metrics(node) = (pr, di, sh, dep + c)
        }
      }

      (1, betweenness)
    }


    var all_betweenness = betweenness.reduceByKey{ case (map1, map2) => map1 ++= map2.map{ case (k, v) =>
      k -> (v + map1.getOrElse(k, 0.0))
    }}.flatMap { case (_, edge) => edge
      .filter(edge => edge._1._1 < edge._1._2)
      .map(edge => (edge._1._1, edge._1._2, edge._2))}//.sortBy(edge => (edge._1, edge._2))


    //Making betweenness in the right format
    var bcastId_map = sc.broadcast(id_map.collectAsMap())
    /*
    var final_betweenness = all_betweenness.map{ case edge =>
      (bcastId_map.value.getOrElse(edge._1, 0),bcastId_map.value.getOrElse(edge._2, 0), edge._3)
    }
    */
    var final_betweenness = all_betweenness.map(edge => (edge._1, (edge._2, edge._3))).join(id_map)
                                          .map(edge => (edge._2._1._1, (edge._2._2, edge._2._1._2))).join(id_map)
                                          .map(edge => (edge._2._1._1, edge._2._2, edge._2._1._2))
                                          .sortBy(edge => (edge._1, edge._2))


    // Writing it to a output_path
    val writer = new PrintWriter(new File(output_location))
    for(each <- final_betweenness.collect()){
      writer.write("(" + each._1 + "," + each._2 + "," + each._3 + ")\n")
    }
    writer.close()
    //final_betweenness.coalesce(1).saveAsTextFile(output_location)


    // Getting rid off the zipIds
    //var new_vertices = graph.vertices.join(id_map).map{ case vertex => (vertex._2._2.toLong, vertex._2._1)}
    var new_vertices = graph.vertices.map{ case vertex =>
      (bcastId_map.value.getOrElse(vertex._1, 0).toLong, 1)
    }
    var new_edges =  final_betweenness.map(edge => Edge(edge._1, edge._2, 1))
    var new_graph = Graph(new_vertices, new_edges)


    (new_graph, final_betweenness.map(edge => (edge._1.toLong, edge._2.toLong, edge._3)))
  }


  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    //println("start: " + start)

    println("In betweenness object. ")
    val conf = new SparkConf().setAppName("Heet_Sheth_HW5_betweenness").setMaster("local")
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    // Args
    val input_file = args(0)//"dataset/ratings.csv"//
    val betweenness_output = args(1)//"dataset/Heet_Sheth_betweenness.txt"//

    // Hyper-params
    val threshold = 3 // 3 commonly rated movies

    // makeGraph from commonly rated movie from ratings.csv file
    var (graph_with_ids, id_map) = makeGraph(sc, threshold, input_file)
    println("Done making graph from ratings.csv file")

    // Calculate betweenness
    var (graph, graph_betweenness) = calculateBetweeness(sc, graph_with_ids, id_map, betweenness_output)
    println("Done calculating betweenness!")

    var end = System.currentTimeMillis()
    //println(end)
    println("The total execution time taken is " + ((end-start)/1000.0).toString + " sec.")
  }

}
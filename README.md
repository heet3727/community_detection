# community_detection

This project implements a community detection algorithm using divisive hierarchical clustering. [(Girvan-Newman algorithm)](https://en.wikipedia.org/wiki/Girvan%E2%80%93Newman_algorithm)

## Overview
This will make use of [graphX](http://spark.apache.org/docs/latest/graphx-programming-guide.html) library. The whole algorithm can be divided into two modules. The first, Heet_Sheth_Betweenness.scala, calculates the betweenness from the data. The other, Heet_Sheth_Community.scala, finds disjoint communities from the calculated betweenness between the pairs. 
Output folder has two files. Heet_Sheth_betweenness.txt has betweenness values and Heet_Sheth_communities.txt has final communities with the highest modularity value.

## Algorithm:
1.	Calculate betweenness using normal bfs search
2.	Remove 1000 edges in each iteration
3.	Run step 2 until we get the highest modularity


## Analysis of data used:
The network has 671 nodes with 154331 edges. This tells that the given graph is densely connected. After running community detection algorithm, we got 23 communities according to GN algorithm. We see one giant community with almost all the nodes and the rest are just a couple of nodes or single nodes in the community. 
### Conclusion: 
The GN algorithm removes new users from the network rather than making proper community. GN is not the best algorithm to detect communities.

### Output:
For Betweenness: The total execution time taken is 92.1 sec.
For Community: The total execution time taken is 160.783 sec.

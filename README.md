# community_detection

This project implements a community detection algorithm using divisive hierarchical clustering. [(Girvan-Newman algorithm)](https://en.wikipedia.org/wiki/Girvan%E2%80%93Newman_algorithm)

## Overview
This will make use of [graphX](http://spark.apache.org/docs/latest/graphx-programming-guide.html) library. <br />
The whole algorithm can be divided into two modules. The first, Heet_Sheth_Betweenness.scala, calculates the betweenness from the data. The other, Heet_Sheth_Community.scala, finds disjoint communities from the calculated betweenness between the pairs. <br />
Output folder has two files. Heet_Sheth_betweenness.txt has betweenness values and Heet_Sheth_communities.txt has final communities with the highest modularity value.

## Algorithm:
1.	Calculate betweenness value for each edge using bfs search
2.	Remove 1000 edges in each iteration
3.	Run step 2 until we get the highest modularity

## Analysis of data used:
Used movieLens dataset with 671 users and their ratings. Each user represents a user. If the count of the commonly rated movie between any two users is greater than 3, (hyper-param) an edge is considered. There are total 154331 such edges present in the network. This tells that the given graph is densely connected. 

### Configurations:
scala v2.10 <br />
spark-1.6.2 <br />
graphX library to manipulate graph

### Output:
To calculate betweenness, it takes around 90 seconds to run for the used dataset. <br />
To detect communities, it takes total 160 seconds. 

## Conclusion: 
We get 23 communities according to GN algorithm. We see one giant community with almost all the nodes and the rest are just a couple of nodes or single nodes in the community. The GN algorithm removes new users from the network rather than making proper community. GN is not the best algorithm to detect communities.



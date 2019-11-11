# Kaypher

Kaypher is Cypher for Kafka Graphs

## Creating a Kafka Graph

A graph in Kafka Graphs is represented by two tables from Kafka Streams, one for vertices and one for edges. The vertex table is comprised of an ID and a vertex value, while the edge table is comprised of a source ID, target ID, and edge value.

```
KTable<Long, Long> vertices = ...
KTable<Edge<Long>, Long> edges = ...
KGraph<Long, Long, Long> graph = new KGraph<>(
    vertices, 
    edges, 
    GraphSerialized.with(Serdes.Long(), Serdes.Long(), Serdes.Long())
);
```

For example, in the class ```TestGraphUtils``` we define the following graph

![Graph](images/Graph.png?raw=true)

- Its ```vertices``` are defined by:
   ```
   1,1
   2,2
   3,3
   4,4
   5,5
   6,6
   ```

- Its ```edges``` are defined by:
   ```
   1,2,12
   1,3,13
   2,3,23
   3,4,34
   3,6,36
   4,5,45
   4,6,46
   5,6,56
   6,1,61
   ```

## Creating Kafka Graphs with Kaypher

Adding data in Cypher works very similarly to any other data access language’s insert statement. Instead of the ```INSERT``` keyword like in SQL, though, Cypher uses ```CREATE```. You can use ```CREATE``` to insert nodes, relationships, and patterns into Kafka Graphs.

- To create the above ```vertices``` using the ```CREATE``` statement:

   ```
   CREATE (vertex:Vertex {name: '1'})
   RETURN vertex
  
   CREATE (vertex:Vertex {name: '2'})
   RETURN vertex
  
   CREATE (vertex:Vertex {name: '3'})
   RETURN vertex
  
   CREATE (vertex:Vertex {name: '4'})
   RETURN vertex
  
   CREATE (vertex:Vertex {name: '5'})
   RETURN vertex
  
   CREATE (vertex:Vertex {name: '6'})
   RETURN vertex
   ```

- To create the ```edges``` first we use the ```MATCH``` statement to name the Vertices and then ```CREATE``` to add the Edges:

   ```
   MATCH (one:Vertex {name: '1'})
   MATCH (two:Vertex {name: '2'})
   MATCH (three:Vertex {name: '3'})
   MATCH (four:Vertex {name: '4'})
   MATCH (five:Vertex {name: '5'})
   MATCH (six:Vertex {name: '6'})

   CREATE (one)-[rel:'12']->(two) 
   CREATE (one)-[rel:'13']->(three) 
   CREATE (two)-[rel:'23']->(three) 
   CREATE (three)-[rel:'34']->(four) 
   CREATE (three)-[rel:'36']->(six)     
   CREATE (four)-[rel:'45']->(five)     
   CREATE (four)-[rel:'46']->(six)
   CREATE (five)-[rel:'56']->(six)       
   ```

## The ```WHERE``` clause

You can write a query that looks for specific values, just like with the ```MATCH``` clause, but you can also use the ```WHERE``` clause in the same manner. Both queries execute with the same performance.

   ```
    //query using equality check in the MATCH clause
    MATCH (v:Vertex {name: 'one'})
    RETURN v
    
    //query using equality check in the WHERE clause
    MATCH (v:Vertex)
    WHERE v.name = 'one'
    RETURN v
   ```


### Negating Properties
Sometimes, you may want to return results that do not match a property value. In this case, you need to search for where value is not something using ```WHERE NOT```.

There are a few types of these comparisons that you can run in Cypher with the standard boolean operators ```AND```, ```OR```, ```XOR```, and ```NOT```. To show a comparison using ```NOT```, we can write the opposite of the query example just above.

   ```
    //query using inequality check in the WHERE clause    
    MATCH (v:Vertex)
    WHERE NOT v.name = 'one'
    RETURN v
   ```

### Testing if a Property Exists
You may only be interested if a property exists on a node or relationship. To write this type of existence check, you simply need to use the ```WHERE``` clause and the ```exists()``` method for that property.

   ```
    //Query1: find all the vertices who have a grade property
    MATCH (v:Vertex)
    WHERE exists(v.grade)
    RETURN v.name
    
    //Query2: find all the relationships that have a weight property
    MATCH (v1:Vertex)-[rel:]->(v2:Vertex)
    WHERE exists(rel.weight)
    RETURN v1, rel, v2
   ```

###Querying Ranges of Values
There are frequently queries where you want to look for data within a certain range. Date or number ranges can be used to check for events within a certain timeline.
The syntax for this criteria is very similar to SQL and other programming language logic structures for checking ranges of values.
    
   ```
    //Find all the vertex that have a weight between 3 and 7 inclusively
    MATCH (v:Vertex)
    WHERE 3 <= v.weight <= 7
    RETURN v
   ```

## Graph Operations

Kafka Graphs provides a number of APIs for transforming graphs in the same manner as Apache Flink Gelly and Apache Spark GraphX.  

- Filtering methods
  - ``filterOnEdges``
  - ``filterOnVertices``
  - ``subgraph``
- Joining methods
  - ``joinWithEdges``
  - ``joinWithEdgesOnSource``
  - ``joinWithEdgesOnTarget``
  - ``joinWithVertices``
- Mapping methods
  - ``mapEdges``
  - ``mapVertices``
- Reducing methods
  - ``groupReduceOnEdges``
  - ``groupReduceOnNeighbors``
  - ``reduceOnEdges``
  - ``reduceOnNeighbors``
- Transforming methods
  - ``inDegrees``
  - ``outDegrees``
  - ``undirected``

For example, the following will compute the sum of the values of all incoming neighbors for each vertex.

```
graph.reduceOnNeighbors(new SumValues(), EdgeDirection.IN);
```

## Running Graph Operations

In the class ```GraphOperations``` we run the Operations over the defined Graph

- The method ```testOutDegrees()``` 
  
  Calculates ```KTable<Long, Long> outDegrees = graph.outDegrees();```
  
  The result is the out-degree of each vertex:
     ```
     1,2
     2,1
     3,2
     4,2
     5,1
     6,1
     ```

- The method ```testInDegrees()```

  Calculates ```KTable<Long, Long> inDegrees = graph.inDegrees();```

  The result is the in-degree of each vertex:
     ```
     1,1
     2,1
     3,2
     4,1
     5,1
     6,3
     ```

- The method ```testUndirected()```

  Calculates ```KTable<Edge<Long>, Long> data = graph.undirected().edges();```

  The result is the all the edges without considering direction:
     ```
     1,2,12    2,1,12
     1,3,13    3,1,13
     2,3,23    3,2,23
     3,4,34    4,3,34
     3,6,36    6,3,36
     4,5,45    5,4,45
     4,6,46    6,4,46
     5,6,56    6,5,56
     6,1,61    1,6,61
     ```


- The method ```testSubGraph()```

  Calculates ```KTable<Edge<Long>, Long> data = graph.subgraph((k, v) -> v > 2, (k, e) -> e > 34).edges();```

  The result is all the edges with vertex greater than 2 and edge greater than 34:
     ```
     3,6,36
     4,5,45
     4,6,46
     5,6,56
     ```

- The method ```testFilterVertices()```

  Calculates ```KTable<Edge<Long>, Long> data = graph.filterOnVertices((k, v) -> v > 2).edges();```

  The result are all the vertex greater that 2:
    ```
    3,4,34
    3,6,36
    4,5,45
    4,6,46
    5,6,56
    ```


- The method ```testFilterEdges()```

  Calculates ```KTable<Edge<Long>, Long> data = graph.filterOnEdges((k, e) -> e > 34).edges();```

  The result are all the edges greater than 34:
    ```
    4,5,45
    4,6,46
    5,6,56
    6,1,61
    ```
  
 ## Running Graph Operations with Kaypher

- To list all the Edges in the graph: 
    ```
    MATCH (v1:Vertex)-[r:]->(v2:Vertex)
    RETURN v1.name, r.name, v2.name
    ```

- To list all the Out edges of the Vertex ```one```: 
    ```
    MATCH (v1:Vertex)-[r:]->(v2:Vertex)
    WHERE v1.name = 'one'
    RETURN v1.name, r.name, v2.name
    ```
     
- To list all the In edges of the Vertex ```one```:
    ```
    MATCH (v1:Vertex)-[r:]->(v2:Vertex)
    WHERE v2.name = 'one'
    RETURN v1.name, r.name, v2.name
    ```

-  List all the edges with vertex greater than 2 and edge greater than 34:
    ```
    MATCH (v1:Vertex)-[r:]->(v2:Vertex)
    WHERE v1.value > 2
    AND r.value > 34
    RETURN v1.name, r.name, v2.name
    ```

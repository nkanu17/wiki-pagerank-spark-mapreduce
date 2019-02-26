# Project Title

This project computes the page rank of Wikipedia edit history using MapReduce chaining before a specific date (user specified input).
The program will then consist of total of three MapReduce jobs where the first one will parse the documents and the second job will calculate the page rank in iterations (user specified input). The final job will output the Wikipedia article with its page rank. Upon the completion of each job, the output is written onto different folders that become the input folders for the next MapReduce job. After the completion of the final job, the output is available to view through the HDFS output folders.

## Solution

### INPUT REQUIREMENTS:
There are four input arguments 
1. Input path (Path of the Wikipedia edit history)
2. Output folder name (must be unique)
3. Number of iterations to compute page rank (Integer)
4. Date (ISO 8601 Format)

Example from terminal: hadoop mapreduce.PageRank /user/enwiki/enwiki-20080103-sample.txt pagerank1 5 2004-01-01T00:00:00Z

### IMPLEMENTATION:

The program get intitiates from PageRank class' main method. From the run method of PageRank class, other three jobs get executed one after another, as described below:

PageRank.class -> Job1 -> Job2 -> Job3

#### Job1 
- ParseMapper (mapper class)
- ParseReducer (reducer class)

Description:
In this job, the ParseMapper will parse through the input Wiki edit file line by line, removes self-loops and duplicate outlinks, and will produce an output that consists of the article title, revision date, and all of its unique outlinks.
This output becomes the ParseReducer's input where it filters the dates of the revisions based on the user input, creates an initial page rank of 1.0 and produces an out put that consists of article title, intial page rank, outlinks.

Objective:
- Parse through wikipedia edit pages line by line
- Filter articles with revision dates after the user specified time
- Remove self loops (remove references of an article to itself)
- Remove duplicate outlink pages (remove references of an article if that reference occurs more than once)

Final Output:
- sdsds


#### Job2
- RankMapper (mapper class)
- RankReducer (reducer class)

#### Job3
- OutputMapper (mapper class)




The mapper's input is <key:line number as LongWritable key, value: wiki file line as Text>
Output is <key: article name as Text, value: revision date and outlinks as TextArrayWritable>
The combiner's input is  <key: article name as Text, value: revision date and list of outlinks as TextArrayWritable>
Output is <key: article name a Text, value: filtered outlinks as TextArrayWritable>
The first reducer's input is <key: article name as Text, value: list of outlinks as TextArrayWritable>
Output is <key: article name as Text, value: filtered outlinks as Text and an additional column for page rank as 1.0 for every article id as default>
Configuration: number of mappers is default.

Phase 2:(LoopingPageRank)
Second mapper reducer
The classes involved in this phase are-
- LastReducer (job configuration class and includes mapper-reducer class subclasses)

The second phase is responsible for:-
- Represent the article, its outlinks and rank as a Node and initialize it.
- Iteratively calculate the Page rank for each article based on the input supplied by the user.
- For the last loop, update the output to article_title and score.

The second map-reduce implements the page rank algorithm. In this the mapper reads the article, initial page rank (1) and outlink details and converts them to a node. It is also responsible to supply the node and it children with an updated page rank. The reducer computes the actual rank of the node using its inlinks and the Page Rank formula for the article by summing up each of the outlink page rank and multiplying the damping factor.

The mapper's input is <key:line number as Text, value: article name   current pagerank   outlinks as Text>
Output is <key: article name as Text, value: article title, pagerank, outlinks as Text>
The second reducer's input is <key: article name as Text,pagerank, value: outlinks as Text>
Output is <key: article name as Text,  value: new rank, outlinks as Text>
Configuration: number of mappers is default.

Phase 3:
The classes involved in this phase are-
- FinalRun (job configuration class also includes the mapper and reducer sub classes)

The third phase is responsible for-
- Combines the first map reduce with the second map reduce using the loop
- Gives the output in the format specified on the basis of rounds.


## Assumptions

## Optimizations
(b) any interesting aspects of your solution
(e.g., assumptions you’ve made, optimisations that you thought of, etc.)
## Modifications
, as well as (c) any
modifications you may have done to the provided project/maven build files.


# Wiki PageRank with MapReduce and Spark

## Assumptions

- PageRank formula: PR(u)=0.15 + 0.85 * Sum(PR(v)/L(v)), ∀v: ∃(v,u) ∈S, where L(v) is the number of out-links of page v
- First the first iteration, the page rank of all articles is 1.0
- After each iteration, the page rank score of an articule u  is sum of all the contributions of articles with an outlink to u, times 0.85, plus 0.15 (= 1 - 0.85). The 0.85 factor is called the “damping factor” of PageRank. (Given by University of Glasgow's Nikos Ntarmos)
- Assumed 'REVISION' and 'MAIN' occur at the beginning of a new line

## Assumed Format of Wiki revision history pages

Each revision history record consists of 14 lines, each starting with a tag and containing a space/tabdelimited series of entries. More specifically, each record contains the following data/tags, one tag
per line:
- REVISION: revision metadata, consisting of:
- article_id: a large integer, uniquely identifying each page.
- rev_id: a large number uniquely identifying each revision.
- article_title: a string denoting the page’s title (and the last part of the URL of the
page, hence also uniquely identifying each page).
- timestamp: the exact date and time of the revision, in ISO 8601 format; e.g., 13:45:00
UTC 30 September 2013 becomes 2013-09-12T13:45:00Z, where T separates the
date from the time part and Z denotes the time is in UTC. (Note: a class that translates
such dates into numerical form is provided in the skeleton code).
- [ip:]username: the name of the user who performed the revision, or her DNS-resolved
IP address (e.g., ip:office.dcs.gla.ac.uk) if anonymous.
- user_id: a large number uniquely identifying the user who performed the revision, or
her IP address as above if anonymous.
- CATEGORY: list of categories this page is assigned to.
- IMAGE: list of images in the page, each listed as many times as it occurs.
- MAIN, TALK, USER, USER_TALK, OTHER: cross-references to pages in other namespaces.
- EXTERNAL: list of hyperlinks to pages outside Wikipedia.
- TEMPLATE: list of all templates used by the page, each listed as many times as it occurs.
- COMMENT: revision comments as entered by the revision author.
- MINOR: a Boolean flag (0|1) denoting whether the edit was marked as minor by the author.
- TEXTDATA: word count of revision's plain text.
- An empty line, denoting the end of the current record. 



## Spark
Info coming

## Map Reduce
This part computes the page rank of Wikipedia edit history using MapReduce chaining before a specific date (user specified input).
The program will then consist of total of three MapReduce jobs where the first one will parse the documents and the second job will calculate the page rank in iterations (user specified input). The final job will output the Wikipedia article with its page rank. Upon the completion of each job, the output is written onto different folders that become the input folders for the next MapReduce job. After the completion of the final job, the output is available to view through the HDFS output folders.

### INPUT REQUIREMENTS:
There are four input arguments 
1. Input path (Path of the Wikipedia edit history)
2. Output folder name (must be unique)
3. Number of iterations to compute page rank (Integer)
4. Date (ISO 8601 Format)

Example from terminal: hadoop mapreduce.PageRank /user/enwiki/enwiki-20080103-sample.txt pagerank1 5 2004-01-01T00:00:00Z

## IMPLEMENTATION:

The program get intitiates from PageRank class' main method. From the run method of PageRank class, other three jobs get executed one after another, as described below:

PageRank.class -> Job1 -> Job2 -> Job3

### Job1 
- ParseMapper (mapper class)
- ParseReducer (reducer class)

Description:
- In this job, the ParseMapper will parse through the input Wiki edit file line-by-line, removes self-loops and duplicate outlinks, and will produce an output that consists of the article title, revision date, and all of its unique outlinks.- 
- This output becomes the ParseReducer's input where it filters the dates of the revisions based on the user input, creates an initial page rank of 1.0 and produces an out put that consists of article title, intial page rank, outlinks.

Objective:
- Parse through wikipedia edit pages line-by-line
- Filter articles with revision dates after the user specified time
- Remove self loops (remove references of an article to itself)
- Remove duplicate outlink pages (remove references of an article if that reference occurs more than once)

ParseMapper input and output
- Input: <key:line/index number as LongWritable, value: input file line as Text>
- Output: <key: article title as Text, value: date and list of outlinks as TextArrayWritable>

ParseReducer input and output
- Input: <key: article title as Text, date and list of outlinks as TextArrayWritable>
- Output <key: article title as Text, value: initial page rank of 1.0 and list of outlinks as TextArrayWritable>

### Job2
- RankMapper (mapper class)
- RankReducer (reducer class)

Description:
- This job computes the page rank algorithm (shown under assumptions). 
- For the first iteration, RankMapper receives the input from ParseReducer's output, which is the article title, initial page rank of 1.0, and list of outlinks. For all other iterations, the output is given from the RankReducer's previous output, this is dependent on the number of iterations in the user input. Then the mapper converts all articles and outlinks into a node with children: key (which is the outlink article), and value (page rank and article (inlink) that references the outlink) in a loop but for the final loop sends the original input as the output. 
- The RankReduer receives the output from the RankMapper and computes the page rank according to each node and its children using the formula (shown under assumptions). The output is the article title, page rank, and outlinks (if present) for every article present.

Objective:
- Each article, outlinks, rank get initialized as a node (as in an article and each outlink get represented as a node)
- Calculate page rank iteratively for each node
- Output the article title and page rank for unique article title.

RankMapper input and output
- Input: <key: line/index number as Text, page rank and list of outlinks as TextArrayWritable>
- Output: <key: article title as Text, value: page rank and inlink/outlinks as TextArrayWritable>

RankReducer input and output
- Input: <key: line/index number as Text, page rank and list of outlinks as TextArrayWritable>
- Output <key: article title as Text, value: page rank and list of outlinks as TextArrayWritable>

### Job3
- OutputMapper (mapper class)

Description:
- This is the final job where the final output of the program will be produced. 
- The output consists of each article and its page rank.

OutputMapper Input and Output
- Input: <key: line/index number as Text, page rank and list of outlinks as TextArrayWritable>
- Output: <key: article title as Text, value: page rank as TextArrayWritable>


## Future Optimizations
- Use tokenizer instead of string splits to make the program faster
- Use a combiner to alleviate memory strain on reducer
- Use another mapper or reducer that would break Job2 in different areas to create a new job that would create the nodes and send outlinks seperately
- To boost performance, find ways to reduce data read by ParseMapper by creation of a sequential file that would send blocks of wiki article details.


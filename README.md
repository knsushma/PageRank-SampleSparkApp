# SampleSparkApp
A Sample Spark Application to compute pageRank on wiki DataSet

##### Assumptions: 
1. `Hadoop` and `Spark` are installed on machine with all required configurations (like NameNode, DataNode, Master and slaves updated)
    Verify by running `jps` that it gives the output
    ````
    23792 Worker
    24340 Jps
    23511 SecondaryNameNode
    23319 DataNode
    24249 CoarseGrainedExecutorBackend
    24171 SparkSubmit
    23164 NameNode
    23676 Master
    ````
2. `sbt` is already installed.
If not, follow the steps below to setup the installation
    ###### Installing sbt on Linux based system

    `echo "deb https://dl.bintray.com/sbt/debian/" | sudo tee -a /etc/apt/sources.list.d/sbt.list`
    `sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823`
    `sudo apt-get update`
    `sudo apt-get install sbt`

3. All files are assumed to be on `HDFS`. So be sure by listing files on HDFS `hdfs dfs -ls <path>`. The required files
        * `export.csv` for Simple Sort Application (Part 2) and
        * `enWikiData` directory containing the first 10 `xml` files are on hdfs
        
##### Note:
Output files would get stored on HDFS in home directory `/`
The Spark Assignment for this submission has been :
        *  Programmed in `Scala`
        *  Uses `sbt` interactive build tool to build a jar
        *  Uses `spark-submit` shell script to run spark application

Unzip the `CS744Assignment1.zip` provided by us by running :
`tar -xvzf CS744Assignment1.tar.gz`

Change Directory to `CSS744SparkApp` by `cd CSS744SparkApp` and executing:
`sbt package` which will build a new jar (Take note that it is run from CS744SparkApp directory)

### Part 1:
Hope the above steps are working fine and you are ready with the setup.

### Part 2:
## Sort 
* Sort using `Data Frame API` for computation:
Code: Present in class `SortDataFrame` in the `CSS744SparkApp`
Input: CSV File to be sorted (assumed to be on `hdfs`)
Output: CSV File containing sorted(by country and timestamp) records.

    Running the following command would execute the application:
    ```
    <PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "SortDataFrame" --master spark://<HOST_IP>:7077 target/scala-2.11/samplespark_2.11-1.0.jar hdfs://<HOST_IP>:9000/export.csv hdfs://<HOST_IP>:9000/dfSortDataSetResult --driver-memory 8G --executor-memory 8G
    ```
    where
    - `<PATH_TO_SPARK_BIN_DIRECTORY>` is the location of Spark bin directory in your machine (would be mostly be `~/spark-2.2.0-bin-hadoop2.7/bin/`),
    - `<HOST_IP>` is the hostname of the `Master` node and you can find in your current machine by running `hostname -i` and
    - `hdfs://<HOST_IP>:9000/export.csv` is the location of input csv file on the HDFS
    - `hdfs://<HOST_IP>:9000/dfSortDataSetResult` is the output file name (can be anything of your choice)

* To run a program that uses RDD for computation
Code: Present in class `SortRDD` in the `CSS744SparkApp`
Input: CSV File to be sorted (assumed to be on `hdfs`)
Output: CSV File containing sorted(by country and timestamp) records.

    Running the following command would execute the application:
    ```
    <PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "SortRDD" --master spark://<HOST_IP>:7077 target/scala-2.11/samplespark_2.11-1.0.jar hdfs://<HOST_IP>:9000/export.csv hdfs://<HOST_IP>:9000/dfSortDataSetResult --driver-memory 8G --executor-memory 8G
    ```

### Part 3:
## PageRank

Input: Input File or Directory to be sorted (assumed to be on `hdfs`). In case, you are adding the directory path, make sure to add `hdfs://<HOST_IP>:9000/<INPUT_DIR>/*`
Output: The Output File to store the data.

##### Naive Based
```
<PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "PageRankNaive" --master spark://<HOST_IP>:7077  target/scala-2.11/samplespark_2.11-1.0.jar hdfs://<HOST_IP>:9000/<INPUT_FILE_OR_DIR> hdfs://<ip_address>:9000/naivePageRankResult --driver-memory 8G --executor-memory 8G
```
##### Partition Based
```
<PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "PageRankPartition" --master spark://<host IP>:7077  target/scala-2.11/samplespark_2.11-1.0.jar hdfs://<HOST_IP>:9000/<INPUT_FILE_OR_DIR> hdfs://<HOST_IP>:9000/partitionPageRankResult --driver-memory 8G --executor-memory 8G
```
##### Graph Based
```
<PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "PageRankGraph" --master spark://<host IP>:7077  target/scala-2.11/samplespark_2.11-1.0.jar hdfs://<HOST_IP>:9000/<INPUT_FILE_OR_DIR> hdfs://<HOST_IP>:9000/graphPageRankResult <PARTITION_NUMBER> --driver-memory 8G --executor-memory 8G
```

##### Cached Based
```
<PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "PageRankCached" --master spark://<host IP>:7077  target/scala-2.11/samplespark_2.11-1.0.jar hdfs://<HOST_IP>:9000/<INPUT_FILE_OR_DIR> hdfs://<HOST_IP>:9000/cachedPageRankResult <PARTITION_NUMBER> --driver-memory 8G --executor-memory 8G
```
#####  Range Based
```
<PATH_TO_SPARK_BIN_DIRECTORY>/spark-submit --class "PageRankRangePartition" --master spark://<HOST_IP>:7077  target/scala-2.11/samplespark_2.11-1.0.jar hdfs://<HOST_IP>:9000/<INPUT_FILE_OR_DIR> hdfs://<HOST_IP>:9000/rangePageRankResult <PARTITION_NUMBER> --driver-memory 8G --executor-memory 8G
```
where `<PARTITION_NUMBER>` is any integer value.

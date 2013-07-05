javac -classpath hadoop-core-1.0.4.jar -d init_pkg Init.java
jar -cvf init.jar -C init_pkg .
cp ./init.jar /usr/local/hadoop/pagerank_jars

javac -classpath hadoop-core-1.0.4.jar -d pr_pkg PageRank.java
jar -cvf pr.jar -C pr_pkg .
cp ./pr.jar /usr/local/hadoop/pagerank_jars

javac -classpath hadoop-core-1.0.4.jar -d sort_pkg Sort.java
jar -cvf sort.jar -C sort_pkg .
cp ./sort.jar /usr/local/hadoop/pagerank_jars

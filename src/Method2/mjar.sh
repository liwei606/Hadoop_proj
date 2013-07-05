#javac -classpath hadoop-core-1.0.4.jar -d pr_classes PageRank.java
#jar -cvf pr.jar -C pr_classes .
#cp ./pr.jar /usr/local/hadoop/myjars
#javac -classpath hadoop-core-1.0.4.jar -d rc_classes Reconstruct.java
#jar -cvf rc.jar -C rc_classes .
#cp ./rc.jar /usr/local/hadoop/myjars
javac -classpath hadoop-core-1.0.4.jar -d rciv_classes PageRank_initV.java
jar -cvf rciv.jar -C rciv_classes .
cp ./rciv.jar /usr/local/hadoop/myjars
javac -classpath hadoop-core-1.0.4.jar -d sort_classes Sort.java
jar -cvf sort.jar -C sort_classes .
cp ./sort.jar /usr/local/hadoop/myjars

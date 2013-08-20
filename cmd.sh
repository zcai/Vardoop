javac -classpath .:/root/hadoop-1.0.1/hadoop-core-1.0.1.jar:/root/hadoop-1.0.1/commons-logging-1.1.1/commons-logging-1.1.1.jar:/root/hadoop-1.0.1/lib/commons-cli-1.2.jar *.java
jar cef VariantsCaller VariantsCaller.jar *.class
hadoop fs -rmr output
hadoop jar VariantsCaller.jar -Dmapred.map.child.java.opts="-Xmx3000m" -Dmapred.task.timeout=1800000 input output gatk
#hadoop jar VariantsCaller.jar -archives /mnt/karyotypic_order_hg19_ref.tar.gz -Dmapred.map.child.java.opts="-Xmx3000m" -Dmapred.task.timeout=1800000 input output samtools

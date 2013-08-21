#How to compile and run Vardoop

javac -classpath .:/your_path/hadoop-core-1.0.1.jar:/your_path/commons-logging-1.1.1.jar:/your_path/commons-cli-1.2.jar *.java
jar cef Vardoop Vardoop.jar *.class
hadoop fs -rmr output
hadoop jar Vardoop.jar -Dmapred.map.child.java.opts="-Xmx3000m" -Dmapred.task.timeout=1800000 input output gatk
#hadoop jar VariantsCaller.jar -archives /mnt/karyotypic_order_hg19_ref.tar.gz -Dmapred.map.child.java.opts="-Xmx3000m" -Dmapred.task.timeout=1800000 input output samtools

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;

public class VariantsCaller {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: VariantsCaller <inputDir> <outputDir> <samtools|gatk>");
			System.exit(2);
		}
		if (!(otherArgs[2].equalsIgnoreCase("samtools")) && !(otherArgs[2].equalsIgnoreCase("gatk"))){
			System.err.println("Usage: VariantsCaller <inputDir> <outputDir> <samtools|gatk>");
			System.exit(2);
		}

		conf.set("method", otherArgs[2]);//this line must be before the next line
		Job job = new Job(conf, "call variants");
		job.setJarByClass(VariantsCaller.class);
		job.setMapperClass(VariantsMapper.class);
		job.setInputFormatClass(BamInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(0);


		//DistributedCache in the following way not working, sorkaround: use -files option in the command line, such as
		///home/ubuntu/hadoop-1.0.1/bin/hadoop jar bamdrive.jar -files /home/ubuntu/test/parse_bam/xcaicai.txt input output
		//or /home/ubuntu/hadoop-1.0.1/bin/hadoop jar bamdrive.jar -archives /home/ubuntu/test/parse_bam/xcaicai.tar.gz input output
		//there is one more problem with  getLocalCacheFiles in the mapper for sure, need to fix
		//Path hdfsPath = new Path("/user/ubuntu/cache/xcaicai.txt");
		//DistributedCache.addCacheFile(hdfsPath.toUri(),conf);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


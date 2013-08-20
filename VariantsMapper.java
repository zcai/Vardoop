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
import java.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.util.*;

public class VariantsMapper extends Mapper<Object, Text, Text, Text>{
	Text name = new Text();
	static Configuration conf = null;
	public void map(Object key,Text value,Context context)throws IOException, InterruptedException{
		System.out.println("key from mapper: "+key.toString());
		System.out.println("value from mapper: "+value);

		//read a record from the inputSplit into a tmp file
		Long start = new Long(value.toString().split("#")[0]);
		long pre_bgzf_pos = start;
		Long block_size = new Long(value.toString().split("#")[1]);
		long bgzf_pos = start + block_size;
		conf = context.getConfiguration();
		String variant_calling_method = conf.get("method");
		System.out.println("variant_calling_method: "+variant_calling_method);

		String input_bam_file = key.toString();
		System.out.println("file name from mapper: "+input_bam_file);

		FileSystem fs = FileSystem.get(conf);

		//create a new bam file from the hdfs block
		CreateBam bam = new CreateBam();
		long [] pos_values = bam.createStart(input_bam_file,start,block_size,conf);
		System.out.println("split position info from mapper: "+pos_values[0]+" "+pos_values[1]+" "+pos_values[2]+" "+pos_values[3]);
		ReadBGZFBlock bgzf_block = new ReadBGZFBlock();
		String bam_file_split = bgzf_block.readStart(input_bam_file,pos_values[0],(int)pos_values[1],pos_values[2],(int)pos_values[3],conf);

		HashMap<Integer, String> ref_name = bgzf_block.readBamHead(input_bam_file,conf).getRef();

		//create an interval list file
		CreateIntervalList interval_list = new CreateIntervalList();
		int [] interval_info = interval_list.createStart(input_bam_file,start,block_size,conf);
		int count = 2;
		String bcf_file;
		String vcf_file;
		while (interval_info[count] != 0){
			System.out.println("split position info from mapper:"+ref_name.get(new Integer(interval_info[count-2]))+" "+interval_info[count-1]+" "+interval_info[count]);
			String id_to_chrName = ref_name.get(new Integer(interval_info[count-2])).toString().trim();
			String interval_list_file = bam_file_split+"_"+id_to_chrName+"_"+interval_info[count-1]+"_"+interval_info[count]+".interval_list";
			BufferedWriter out = null;
			try{
				out = new BufferedWriter(new FileWriter(interval_list_file));
				//interval list file for samtools
				if (variant_calling_method.equals("samtools")){
					out.write(id_to_chrName+"\t"+interval_info[count-1]+"\t"+interval_info[count]);
				}else{
					//interval list file for GATK
					out.write(id_to_chrName+":"+interval_info[count-1]+"-"+interval_info[count]);
				}
			}catch(Exception e){}
			out.close();


			//variants calling using samtools mpileup
			bcf_file = interval_list_file+".bcf";
			vcf_file = interval_list_file+".vcf";
			if (variant_calling_method.equals("samtools")){
				samtools_mpileup(bam_file_split, interval_list_file, bcf_file, vcf_file);
			}
			//variants calling using GATK Unified Genotyper
			else{
				gatk_genotyper(bam_file_split, interval_list_file, vcf_file);
			}
			//fs.copyFromLocalFile(true,true,new Path(interval_list_file),new Path("output"));
			fs.copyFromLocalFile(true,true,new Path(vcf_file),new Path("output"));
			count += 3;
		}

		//File fbcf = new File(bcf_file);
		//fbcf.delete();
		//fs.copyFromLocalFile(true,false,new Path(bcf_file),new Path("output"));
		//fs.copyFromLocalFile(true,true,new Path(bam_file_split),new Path("output"));

	}

	private void samtools_mpileup(String input_bam_file, String input_interval_list_file, String output_bcf_file, String output_vcf_file){
		String [] cmd = {"/bin/sh","-c","/usr/local/bin/samtools mpileup -l "+input_interval_list_file+" -uf /data/hs37d5.fa "+input_bam_file+"|/usr/local/bin/bcftools view -bvcg - > "+output_bcf_file};//it is recommended to use full path of the tools, otherwise, not working in distributed mode, but working in pseudo distributed mode.
		//use -archives to specify karyotypic_order_hg19_ref.tar.gz
		//String [] cmd = {"/bin/sh","-c","/usr/local/bin/samtools mpileup -l "+input_interval_list_file+" -uf ./karyotypic_order_hg19_ref.tar.gz/karyotypic_order_hg19_ref/karyotypic_order_hg19.fa "+input_bam_file+"|/usr/local/bin/bcftools view -bvcg - > "+output_bcf_file};//it is recommended to use full path of the tools, otherwise, not working in distributed mode, but working in pseudo distributed mode.
		//String [] cmd = {"/bin/sh","-c","/usr/local/bin/samtools mpileup -uf /data/hs37d5.fa "+bam_file_split+"|/usr/local/bin/bcftools view -bvcg - > "+bcf_file};//it is recommended to use full path of the tools, otherwise, not working in distributed mode, but working in pseudo distributed mode.
		//System.out.println(cmd.toString());
		try{
			Process p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			//File fbam = new File(interval_list_file);
			//fbam.delete();

			//String [] cmd2 ={"/bin/sh","-c","/usr/local/bin/bcftools view "+output_bcf_file+"|/usr/local/bin/vcfutils.pl varFilter -D100 > "+output_vcf_file};
			String [] cmd2 ={"/bin/sh","-c","/usr/local/bin/bcftools view "+output_bcf_file+" > "+output_vcf_file};
			Process p2 = Runtime.getRuntime().exec(cmd2);
			p2.waitFor();
		}catch (Exception e){
		}
	}

	private void gatk_genotyper(String input_bam_file, String input_interval_list_file, String output_vcf_file){
		String [] samtools_index = {"/bin/sh","-c","/usr/local/bin/samtools index "+input_bam_file};
		try{
			Process psamtools = Runtime.getRuntime().exec(samtools_index);
			psamtools.waitFor();
			
			String [] cmd = {"/bin/sh","-c","/usr/local/jdk1.6.0_22/bin/java -jar /data/GenomeAnalysisTK-1.6-11-g3b2fab9/GenomeAnalysisTK.jar -R /data/hs37d5.fa -T UnifiedGenotyper -I "+input_bam_file+" -o "+output_vcf_file+" -L "+input_interval_list_file};

			//use -archives to specify karyotypic_order_hg19_ref.tar.gz
			//String [] cmd = {"/bin/sh","-c","/usr/local/jdk1.6.0_22/bin/java -jar /data/GenomeAnalysisTK-1.6-11-g3b2fab9/GenomeAnalysisTK.jar -R ./karyotypic_order_hg19_ref.tar.gz/karyotypic_order_hg19_ref/karyotypic_order_hg19.fa -T UnifiedGenotyper -I "+input_bam_file+" -o "+output_vcf_file+" -L "+input_interval_list_file};
			Process p = Runtime.getRuntime().exec(cmd);
			//System.out.println(arrayToString(cmd,""));
			p.waitFor();
		}catch (Exception e){
		}
	}

public static String arrayToString(String[] a, String separator) {
    StringBuffer result = new StringBuffer();
    if (a.length > 0) {
        result.append(a[0]);
        for (int i=1; i<a.length; i++) {
            result.append(separator);
            result.append(a[i]);
        }
    }
    return result.toString();
}

}




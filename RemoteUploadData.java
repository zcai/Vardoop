import java.net.URL;
import java.net.URI;
import java.net.URLConnection;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.MalformedURLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

class RemoteUploadData {
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: java -classpath .:/usr/local/hadoop-0.20.203.0/lib/commons-logging-1.1.1.jar:/usr/local/hadoop-0.20.203.0/hadoop-core-0.20.203.0.jar:/usr/local/hadoop-0.20.203.0/lib/commons-configuration-1.6.jar:/usr/local/hadoop-0.20.203.0/lib/commons-lang-2.4.jar RemoteUploadData <URL_data_source> <full_path_file_name_on_HDFS> <hdfs_name_URI> <replication factor>");
			System.err.println("For example : java RemoteUploadData ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/pilot2_high_cov_GRCh37_bams/data/NA19240/alignment/NA19240.chrom1.SOLID.bfast.YRI.high_coverage.20100311.bam /user/root/input/test_input.bam hdfs://ip-10-224-53-114.ec2.internal:50001");
			System.err.println("This will upload the file from the ftp site to your hadoop file system.");
			System.exit(2);
		}

		try {
			//String data_file = "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/pilot2_high_cov_GRCh37_bams/data/NA19240/alignment/NA19240.chrom1.SOLID.bfast.YRI.high_coverage.20100311.bam";
			String data_file = args[0];
			URL url = new URL(data_file);
			URLConnection con = url.openConnection();
			long fileSize = Long.parseLong(con.getHeaderField("content-length"));
			
/*
			Map fields = con.getHeaderFields();
			Set set = fields.entrySet();
			Iterator iterator = set.iterator(); 
			while(iterator.hasNext()) {
				Map.Entry me = (Map.Entry)iterator.next(); 
				System.out.print(me.getKey() + ": "); 
				System.out.println(me.getValue()); 
			}
*/
			//InputStream is = con.getInputStream();
			InputStream is = url.openStream();
			//BufferedInputStream bis = new BufferedInputStream(is);

			Configuration conf = new Configuration();
			//file to be created
			//Path file = new Path("/user/root/input/test_input.bam");
			Path file = new Path(args[1]);
			//initiate hdfs
			DistributedFileSystem dfs = new DistributedFileSystem();
			//dfs.initialize(new URI("hdfs://ip-10-224-53-114.ec2.internal:50001"), conf); //fs.default.name
			dfs.initialize(new URI(args[2]), conf);
			FsPermission permissions = new FsPermission("750");
			FSDataOutputStream out = null;
			int bufferSize = 65536000;
			long blockSize = 65536000;//64M
			int totalBlocks = (int)(fileSize/blockSize);
			//System.out.println(totalBlocks);
			boolean overWrite = true;
			try{
				out = dfs.create(file,permissions,overWrite, bufferSize, (short)3, blockSize, null);
			}catch(Exception e){
				e.printStackTrace();
			}

			byte[] buf = new byte[bufferSize];
			int n = is.read(buf);
/*
			while (n >= 0){
				out.write(buf, 0, n);
				System.out.print(n+".");
				n = is.read(buf);
			}
*/
			//dealing with network inputStream, block until the buf is fully filled
			int end = 0;
			double blockRead = 0;//generates double in the division operation, avoid casting
			while (true){
				while (n != buf.length){
					int ret = is.read(buf, n, buf.length - n);
					if (ret == -1) {
						end = 1;
						break;
					}
					n += ret;
				}
				out.write(buf, 0, n);
				blockRead++;
				if (fileSize > 0){
					updateProgress((blockRead/totalBlocks));
				}else{
					System.out.print(".");
				}
				n = 0;
				if (end == 1){
					break;
				}
			}

			out.close();
			is.close();
			//bis.close();

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static void updateProgress(double progressPercentage) {
		final int width = 100; // progress bar width in chars
		System.out.print("\r[");
		int i = 0;
		for (; i <= (int)(progressPercentage*width); i++) {
			System.out.print(">");
		}
		for (; i < width; i++) {
			System.out.print("|");
		}
		System.out.print("]");
	}
}

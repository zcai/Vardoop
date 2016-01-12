import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.Inflater;
import java.util.zip.Deflater;
import java.util.zip.CRC32;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import java.util.*;
public class ReadBGZFBlock{
	public byte [] head = null;//make the array large enough to fit the bam head

	public String readStart(String input_bam_file, long pre_bgzf_pos, int first_pos, long bgzf_pos,int next_pos,Configuration job) {

		FSDataInputStream in = null;
		FileOutputStream fos = null;
		File outf = null;
		try{
			//System.out.println("from ReadBGZFBlock input_bam_file: "+input_bam_file);
			Path file = new Path(input_bam_file);
			FileSystem fs = file.getFileSystem(job);
			in = fs.open(file);
			
			File tmp_folder = new File("/mnt/tmp");
			if (!tmp_folder.exists()){
				try{
					tmp_folder.mkdir();
				}catch(Exception e){System.out.println("failed to create directory /mnt/tmp");}
			}

			outf =  new File("/mnt/tmp/"+new File(input_bam_file).getName()+"."+pre_bgzf_pos+"_"+first_pos+"_"+bgzf_pos+"_"+next_pos+".bam");//store tmp files
			fos = new FileOutputStream(outf);

			int bsize = 0;
			int count = 0;
			long current_bgzf_pos = pre_bgzf_pos;
			in.skip(pre_bgzf_pos);
			while(true){
				if (count == 0){
					//System.out.println("from ReadBGZFBlock.java: write the bam head block");
					//the first bgzf block, which contains only the bam dead
					//write gzip head for bam head
					int bam_head_len = readBamHead(input_bam_file,job).getLength();
					byte [] compressed_bam_head = new byte[bam_head_len];
					Deflater compresserHead = new Deflater(Deflater.DEFAULT_COMPRESSION,true);//BAM does not use wrap,so must use this constructor here
					compresserHead.setInput(head,0,bam_head_len);
					compresserHead.finish();
					int out_len2 = compresserHead.deflate(compressed_bam_head);
					compresserHead.end();//should not use compresser.finish();
		
					byte [] bgzf_head = new byte[] {0x1f,(byte)0x8b,(byte)0x08,0x04};
					fos.write(bgzf_head);
			
					byte [] otherbgzf_head= new byte[] {0x00,0x00,0x00,0x00,0x00,(byte)0xff,0x06,0x00,0x42,0x43};
					fos.write(otherbgzf_head);
			
					byte [] SLEN_head = new byte[] {0x02,0x00};
					fos.write(SLEN_head);
			
					byte [] add_BZISE_head = intToByteArray(out_len2+19+6,2);//6 is XLEN
					fos.write(add_BZISE_head);
			
					fos.write(compressed_bam_head,0,out_len2);//write bam head content
			
					CRC32 checksum_head = new CRC32();
					checksum_head.update(head,0,bam_head_len);//computer original umcompressed data checksum, not compressed data
					byte [] add_CRC32_head = intToByteArray((int)checksum_head.getValue());//calculate the CRC32
					fos.write(add_CRC32_head);
			
					byte [] add_ISIZE_head = intToByteArray(bam_head_len);
					fos.write(add_ISIZE_head);


					//the second bgzf block, which contains the first partial bgzf block
					byte [] other = new byte[10];
					//in.read(other);
					in.readFully(other);
			
					byte [] XLEN = new byte[2];
					//in.read(XLEN);
					in.readFully(XLEN);
					short converted_XLEN = bigToLittleEndianShort(byteArrayToShort(XLEN,0));
					int xlen = (int) converted_XLEN & 0x0000ffff;
					
					byte [] sub = new byte[4];
					//in.read(sub);
					in.readFully(sub);
					
					byte [] BSIZE = new byte[2];
					//in.read(BSIZE);
					in.readFully(BSIZE);
					short converted_BSIZE = bigToLittleEndianShort(byteArrayToShort(BSIZE,0));
					bsize = (int) (converted_BSIZE & 0x0000ffff);
					current_bgzf_pos = current_bgzf_pos + bsize + 1;
					
					//process compressed contents
					byte [] CDATA = new byte[bsize-xlen-19];
					//int r = in.read(CDATA);
					in.readFully(CDATA);
					
					//process the remaining zip metadata
					byte [] CRC32 = new byte[4];
					//in.read(CRC32);
					in.readFully(CRC32);
					//int converted_CRC32 = bigToLittleEndian(CRC32);
					
					byte [] ISIZE = new byte[4];
					//in.read(ISIZE);
					in.readFully(ISIZE);
					int converted_ISIZE = bigToLittleEndian(byteArrayToInt(ISIZE,0));

					//unzip compressed contents using inflate method
					Inflater decompresser = new Inflater(true);//must use true here, since by default BAM do not include the zlib header
					decompresser.setInput(CDATA);
					byte[] content = new byte[converted_ISIZE];
					decompresser.inflate(content);
					decompresser.end();	
					byte [] first_part = new byte[first_pos];
					
					first_part = Arrays.copyOfRange(content, converted_ISIZE-first_pos, converted_ISIZE);
					
					Deflater compresser = new Deflater(Deflater.DEFAULT_COMPRESSION,true);//BAM does not use wrap,so must use this constructor here
					byte [] buffer = new byte[converted_ISIZE];
					compresser.setInput(first_part);
					compresser.finish();
		
					int out_len = compresser.deflate(buffer);
					compresser.end();//should not use compresser.finish();

					if (first_pos != 0){//the first bgzf block contains empty, skip this section, otherwise an empty bam block will be written which indicates the end of the bam file	
						//write gzip header
						byte [] bgzf = new byte[] {0x1f,(byte)0x8b,(byte)0x08,0x04};
						fos.write(bgzf);
						
						byte [] otherbgzf= new byte[] {0x00,0x00,0x00,0x00,0x00,(byte)0xff,0x06,0x00,0x42,0x43};
						fos.write(otherbgzf);
						
						byte [] SLEN = new byte[] {0x02,0x00};
						fos.write(SLEN);
						
						byte [] add_BZISE = intToByteArray(out_len+19+6,2);//6 is XLEN
						fos.write(add_BZISE);
						
						fos.write(buffer,0,out_len);//write bam content

						CRC32 checksum = new CRC32();
						checksum.update(first_part);//computer original umcompressed data checksum, not compressed data
						byte [] add_CRC32 = intToByteArray((int)checksum.getValue());//calculate the CRC32
						fos.write(add_CRC32);
						
						byte [] add_ISIZE = intToByteArray(first_part.length);
						fos.write(add_ISIZE);
					}

					count = 1;
		
					//System.exit(0);	 
				}
				else if (count == 1){
					if (current_bgzf_pos < bgzf_pos){
						byte [] other = new byte[10];
						//in.read(other);
						in.readFully(other);
						fos.write(other);
		
						byte [] XLEN = new byte[2];
						//in.read(XLEN);
						in.readFully(XLEN);
						fos.write(XLEN);
						short converted_XLEN = bigToLittleEndianShort(byteArrayToShort(XLEN,0));
						int xlen = (int) converted_XLEN & 0x0000ffff;
						
						byte [] sub = new byte[4];
						in.readFully(sub);
						fos.write(sub);
		
						 
						byte [] BSIZE = new byte[2];
						in.readFully(BSIZE);
						fos.write(BSIZE);
						short converted_BSIZE = bigToLittleEndianShort(byteArrayToShort(BSIZE,0));
						bsize = (int) (converted_BSIZE & 0x0000ffff);
						current_bgzf_pos = current_bgzf_pos + bsize + 1;
		
						//process compressed contents
						byte [] CDATA = new byte[bsize-xlen-19];
						//int r = in.read(CDATA);
						in.readFully(CDATA);
						fos.write(CDATA);
		
						
						//process the remaining zip metadata
						byte [] CRC32 = new byte[4];
						//in.read(CRC32);
						in.readFully(CRC32);
						fos.write(CRC32);
						//int converted_CRC32 = bigToLittleEndian(CRC32);
						
						byte [] ISIZE = new byte[4];
						//in.read(ISIZE);
						in.readFully(ISIZE);
						fos.write(ISIZE);
						int converted_ISIZE = bigToLittleEndian(byteArrayToInt(ISIZE,0));
		
					}
					if(current_bgzf_pos == bgzf_pos){
						count = 2;
					}
				}else{
					//the final bgzf block, which contains the next partial bgzf block
					byte [] other = new byte[10];
					//in.read(other);
					in.readFully(other);
		
					byte [] XLEN = new byte[2];
					//in.read(XLEN);
					in.readFully(XLEN);
					short converted_XLEN = bigToLittleEndianShort(byteArrayToShort(XLEN,0));
					int xlen = (int) converted_XLEN & 0x0000ffff;
				
					byte [] sub = new byte[4];
					//in.read(sub);
					in.readFully(sub);
				 
					byte [] BSIZE = new byte[2];
					//in.read(BSIZE);
					in.readFully(BSIZE);
					short converted_BSIZE = bigToLittleEndianShort(byteArrayToShort(BSIZE,0));
					bsize = (int) (converted_BSIZE & 0x0000ffff);
					//System.out.println(">BSIZE:"+bsize+"<");
					current_bgzf_pos = current_bgzf_pos + bsize + 1;
		
					//process compressed contents
					byte [] CDATA = new byte[bsize-xlen-19];
					//int r = in.read(CDATA);
					in.readFully(CDATA);
		
					//process the remaining zip metadata
					byte [] CRC32 = new byte[4];
					//in.read(CRC32);
					in.readFully(CRC32);
					//int converted_CRC32 = bigToLittleEndian(CRC32);
				
					byte [] ISIZE = new byte[4];
					//in.read(ISIZE);
					in.readFully(ISIZE);
					int converted_ISIZE = bigToLittleEndian(byteArrayToInt(ISIZE,0));
		
					//unzip compressed contents using inflate method
					Inflater decompresser = new Inflater(true);//must use true here, since by default BAM do not include the zlib header
					decompresser.setInput(CDATA);
					byte[] content = new byte[converted_ISIZE];
					decompresser.inflate(content);
					decompresser.end();	
					byte [] next_part = new byte[next_pos];
					next_part = Arrays.copyOfRange(content, 0, next_pos);
					//fos.write(next_part);  
					
					
					Deflater compresser = new Deflater(Deflater.DEFAULT_COMPRESSION,true);
					byte [] buffer = new byte[converted_ISIZE];
					compresser.setInput(next_part);
					compresser.finish();
					int out_len = compresser.deflate(buffer);
					compresser.end();//should not use compresser.finish();
					
					if (next_pos != 0){//the last bgzf block contains empty, skip this section, otherwise an empty bam block will be written which indicates the end of the bam file
					//write gzip header
					byte [] bgzf = new byte[] {0x1f,(byte)0x8b,(byte)0x08,0x04};
					fos.write(bgzf);
					byte [] otherbgzf= new byte[] {0x00,0x00,0x00,0x00,0x00,(byte)0xff,0x06,0x00,0x42,0x43};
					fos.write(otherbgzf);
					byte [] SLEN = new byte[] {0x02,0x00};
					fos.write(SLEN);
					
					byte [] add_BZISE = intToByteArray(out_len+19+6,2);//6 is XLEN
					fos.write(add_BZISE);
					
					fos.write(buffer,0,out_len);//write bam content
					
					CRC32 checksum = new CRC32();
					checksum.update(next_part);//computer original umcompressed data checksum, not compressed data
					byte [] add_CRC32 = intToByteArray((int)checksum.getValue());//calculate the CRC32
					fos.write(add_CRC32);
					
					byte [] add_ISIZE = intToByteArray(next_part.length);
					fos.write(add_ISIZE);
					}
					byte [] EOF = new byte[] {(byte)0x1F,(byte)0x8B,(byte)8,(byte)4,0,0,0,0,0,(byte)0xFF,6,0,(byte)0x42,(byte)0x43,2,0,(byte)0x1B,0,3,0,0,0,0,0,0,0,0,0};
					fos.write(EOF);            
					break;
				}
			}
		}catch(Exception e){}
		finally{
			if (in != null){
				try{
					in.close();
				}catch(Exception ex){}
			}
			if (fos != null){
				try{
					fos.close();
				}catch(Exception ex2){}
			}
		}
		return outf.toString();
	}

	public BamHead readBamHead(String bamFile,Configuration job){
		//bam head length and reference sequence id~name mappings are stored in the returned object bam_head
		int bam_head_len = 0;
		HashMap<Integer, String> ref_name = new HashMap<Integer, String>();
		BamHead bam_head = new BamHead();
		try{
			Path file = new Path(bamFile);
			System.out.println("readBamHead: "+file);
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream in = fs.open(file);

			//DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(bamFile)));
			//read the first bgzf block
			byte [] other = new byte[10];
			in.read(other);
	
			byte [] XLEN = new byte[2];
			in.read(XLEN);
			short converted_XLEN = bigToLittleEndianShort(byteArrayToShort(XLEN,0));
			int xlen = (int) converted_XLEN & 0x0000ffff;
			
			byte [] sub = new byte[4];
			in.read(sub);
	
			byte [] BSIZE = new byte[2];
			in.read(BSIZE);
			short converted_BSIZE = bigToLittleEndianShort(byteArrayToShort(BSIZE,0));
			int bsize = (int) (converted_BSIZE & 0x0000ffff);
	
			//process compressed contents
			byte [] CDATA = new byte[bsize-xlen-19];
			int r = in.read(CDATA);
	
			//process the remaining zip metadata
			byte [] CRC32 = new byte[4];
			in.read(CRC32);
			//int converted_CRC32 = bigToLittleEndian(CRC32);
			
			byte [] ISIZE = new byte[4];
			in.read(ISIZE);
			int converted_ISIZE = bigToLittleEndian(byteArrayToInt(ISIZE,0));			
			
			//unzip compressed contents using inflate method
			Inflater decompresser = new Inflater(true);//must use true here, since by default BAM do not include the zlib header
			decompresser.setInput(CDATA);
			byte[] content = new byte[converted_ISIZE];
			int len = decompresser.inflate(content);
			decompresser.end();	
			head = content;
			
			/////////////
			byte [] magic = Arrays.copyOfRange(content, bam_head_len, bam_head_len+4);
			bam_head_len += 4;
			
			byte [] l_text = Arrays.copyOfRange(content, bam_head_len, bam_head_len+4);
			bam_head_len += 4;
			int converted_l_text = bigToLittleEndian(byteArrayToInt(l_text,0));
			
			byte [] text = Arrays.copyOfRange(content, bam_head_len, bam_head_len+converted_l_text);
			bam_head_len += converted_l_text;
			     
			byte [] n_ref = Arrays.copyOfRange(content, bam_head_len, bam_head_len+4);
			bam_head_len += 4;
			int converted_n_ref = bigToLittleEndian(byteArrayToInt(n_ref,0));
	
			for (int i=0;i<converted_n_ref;i++){
				byte [] l_name = Arrays.copyOfRange(content, bam_head_len, bam_head_len+4);
				bam_head_len += 4;
				int converted_l_name = bigToLittleEndian(byteArrayToInt(l_name,0));
				
				byte [] name = Arrays.copyOfRange(content, bam_head_len, bam_head_len+converted_l_name);
				bam_head_len += converted_l_name;
				ref_name.put(new Integer(i),new String(name));
				
				byte [] l_ref = Arrays.copyOfRange(content, bam_head_len, bam_head_len+converted_l_name);
				bam_head_len += 4;
			}
			in.close();
		}catch(Exception e){}
		bam_head.setLength(bam_head_len);
		bam_head.setRef(ref_name);
		//return bam_head_len;
		return bam_head;
	}

    public static int bigToLittleEndian(int bigendian) {   
        ByteBuffer buf = ByteBuffer.allocate(4);   
        buf.order(ByteOrder.BIG_ENDIAN);   
        buf.putInt(bigendian);   
        buf.order(ByteOrder.LITTLE_ENDIAN);   
        return buf.getInt(0);   
    } 
    
    public static short bigToLittleEndianShort(short bigendian) {   
        ByteBuffer buf = ByteBuffer.allocate(2);   
        buf.order(ByteOrder.BIG_ENDIAN);   
        buf.putShort(bigendian);   
        buf.order(ByteOrder.LITTLE_ENDIAN);   
        return buf.getShort(0);   
    }
    
    public static int byteArrayToInt(byte[] b, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (4 - 1 - i) * 8;
            value += (b[i + offset] & 0x000000FF) << shift;
        }
        return value;
    }
    
    public static short byteArrayToShort(byte[] b,int offset) {
        int value = 0;
        for (int i = 0; i < 2; i++) {
            int shift = (2 - 1 - i) * 8;
            value += (b[i + offset] & 0x000000FF) << shift;
        }
        return (short)value;
    }

    public static byte [] intToByteArray(int number){//little endian
    	byte [] byteArray = new byte[4];
    	byteArray[3] = (byte)((number >> 24) & 0xFF);
    	byteArray[2] = (byte)((number >> 16) & 0xFF);
    	byteArray[1] = (byte)((number >> 8) & 0xFF);
    	byteArray[0] = (byte)(number & 0xFF);
    	return byteArray;
    }

    public static byte [] intToByteArray(int number, int len){//little endian
    	byte [] byteArray = new byte[2];
    	byteArray[1] = (byte)((number >> 8) & 0xFF);
    	byteArray[0] = (byte)(number & 0xFF);
    	return byteArray;
    }
}





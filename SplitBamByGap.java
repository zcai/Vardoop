import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.Inflater;
import java.util.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//use ReadBGZFBlock.java to double check the results SplitBamByGap generates 
public class SplitBamByGap{
	private static byte [] pipe_buffer = null;
	private static int pipe_buffer_length = 0;
	private static long bam_position = 0;
	private static long bam_length = 0;
	private static int pipe_buffer_start = 0;
	public static List<InputSplit> getSplits(Path inputBamFile,Configuration conf) {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		FSDataInputStream in = null;
		int hdfs_block_size = 65536000;//64M
		try{
		FileSystem  fs = inputBamFile.getFileSystem(conf);
                in  = fs.open(inputBamFile);
		}catch(Exception c){}
        //current position in the unzipped BAM binary file from original BAM file
		//long bam_position = 0;//use the above global static one
        //current position in the unzipped BAM binary file from original BAM file
		//long bam_position = 0;//use the above global static one
		long mark = 0;
		//current position in the original BAM file
		long bgzf_position = 0;
		//bgzf position of the current bgzf block
		long previous_bgzf_position = 0;
		//size (to the end of unzipped BGZF block) of a part of a BGZF block which will be the first block of the will-be made new BAM file
		int part_previous_size = 0;
		//size (the first part in unzipped BGZF block) of a part of a BGZF block which will be the final block of the will-be made new BAM file
		int part_next_size = 0;
		long previous_bam_length = 0;
		
		int bgzf_block_size = 0;
		int previous_converted_refID = -1;


        int first_seq_pos = 0;
        int last_seq_pos = 0;

    	//begin to read bam header
    	//if (din.available() == 0){
        if (pipe_buffer_length == pipe_buffer_start) {
    		bgzf_block_size = readBGZFBlock(in,pipe_buffer_start,pipe_buffer_length);
    		bgzf_position += bgzf_block_size;
    	}
        //byte [] magic = new byte[4];
        //din.read(magic);
        byte [] magic = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
        pipe_buffer_start += 4;
        System.out.println(">magic:"+(new String((new String(magic)).toCharArray())));
        bam_position += 4;
        
        //byte [] l_text = new byte[4];
        //din.read(l_text);
        byte [] l_text = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
        pipe_buffer_start += 4;
        int converted_l_text = bigToLittleEndian(byteArrayToInt(l_text,0));
        //System.out.println(">l_text:"+converted_l_text);
        bam_position += 4;
        
        //byte [] text = new byte[converted_l_text];
        //din.read(text);
        byte [] text = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+converted_l_text);
        pipe_buffer_start += converted_l_text;
        //System.out.println(">text:"+new String(text));
        bam_position += converted_l_text;
               
        //byte [] n_ref = new byte[4];  
        //din.read(n_ref);
        byte [] n_ref = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
        pipe_buffer_start += 4;
        int converted_n_ref = bigToLittleEndian(byteArrayToInt(n_ref,0));
        //System.out.println(">n_ref:"+converted_n_ref);
        bam_position += 4;
        
        for (int i=0;i<converted_n_ref;i++){
	        //byte [] l_name = new byte[4];
	        //din.read(l_name);
	        byte [] l_name = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
	        pipe_buffer_start += 4;
	        int converted_l_name = bigToLittleEndian(byteArrayToInt(l_name,0));
	        //System.out.println(">l_name:"+converted_l_name);
	        bam_position += 4;

	        //byte [] name = new byte[converted_l_name];
	        //din.read(name);
	        byte [] name = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+converted_l_name);
	        pipe_buffer_start += converted_l_name;
	        //System.out.println(">name:"+new String(name));
	        bam_position += converted_l_name;
	        
	        //byte [] l_ref = new byte[4];
	        //din.read(l_ref);
	        byte [] l_ref = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
	        pipe_buffer_start += 4;
	        int converted_l_ref = bigToLittleEndian(byteArrayToInt(l_ref,0));
	        //System.out.println(">l_ref:"+converted_l_ref);
	        bam_position += 4;
        }
	//System.out.println(bam_length+"---"+bam_position);
	part_previous_size = (int)(bam_length - bam_position);//do not include bam head for the first split, the bam head will be added in ReadBGZFBlock.java	       
 	//System.out.println(part_previous_size+"*******************");
        //begin to read bam body
    	//if (din.available() == 0){
    	if (pipe_buffer_length == pipe_buffer_start) {
    		
    		bgzf_block_size = readBGZFBlock(in,pipe_buffer_start,pipe_buffer_length);
    		//System.out.println(pipe_buffer_length+"---"+pipe_buffer_start);
    		bgzf_position += bgzf_block_size;
    	}        
        
    	
        //while (din.available() != 0){//process each read sequence
	int converted_block_size = 0;
        while (pipe_buffer_length > pipe_buffer_start){
        	//System.out.println("+++++++++++++++++++");
        	int flag = 0;
        	//a bgzf block span within the first 4 bytes of the next bgzf block
        	if (bam_position + 4 > bam_length){
        		//System.out.println(">>>>>>>>>>>4");
        		previous_bam_length = bam_length;
        		bgzf_block_size = readBGZFBlock(in,pipe_buffer_start,pipe_buffer_length);
        		bgzf_position += bgzf_block_size;
        		flag = 1;
        	}
            //byte [] block_size = new byte[4];
            //din.read(block_size);
	        byte [] block_size = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
	        pipe_buffer_start += 4;
            converted_block_size = bigToLittleEndian(byteArrayToInt(block_size,0));
            //System.out.println(">block_size:"+converted_block_size);
            bam_position += 4;
            bam_position += converted_block_size;
            
	        if (bam_position > bam_length){
	        	//System.out.println(">>>>>>>>>>>end");
	        	previous_bam_length = bam_length;
	        	//System.out.println(pipe_buffer_start+">>>>>>>>>>>end"+pipe_buffer_length);
	        	bgzf_block_size = readBGZFBlock(in,pipe_buffer_start,pipe_buffer_length);
	        	//System.out.println(pipe_buffer_length);
	        	bgzf_position += bgzf_block_size;
	        	flag = 1;
	        }
	        
            //byte [] refID = new byte[4];
            //din.read(refID);
	        byte [] refID = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
	        pipe_buffer_start += 4;
            int converted_refID = bigToLittleEndian(byteArrayToInt(refID,0));
            //System.out.println(">refID:"+converted_refID);
            
            //byte [] pos = new byte[4];
            //din.read(pos);
	        byte [] pos = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
	        pipe_buffer_start += 4;
            int converted_pos = bigToLittleEndian(byteArrayToInt(pos,0));
            //System.out.println(">pos:"+converted_pos);
            //last_seq_pos = converted_pos;
            first_seq_pos = converted_pos;
            
            

            /*
            byte [] bin_mq_nl = new byte[4];
            din.read(bin_mq_nl);
            int converted_bin_mq_nl = bigToLittleEndian(byteArrayToInt(bin_mq_nl,0));
            //System.out.println(">bin_mq_nl:"+new Long((long) (converted_bin_mq_nl & 0x00000000ffffffffl)).toString()+"<");

            byte [] flag_nc = new byte[4];
            din.read(flag_nc);
            int converted_flag_nc = bigToLittleEndian(byteArrayToInt(flag_nc,0));
            //System.out.println(">flag_nc:"+new Long((long) (converted_flag_nc & 0x00000000ffffffffl)).toString()+"<");

            byte [] l_seq = new byte[4];
            din.read(l_seq);
            int converted_l_seq = bigToLittleEndian(byteArrayToInt(l_seq,0));
            //System.out.println(">l_seq:"+converted_l_seq);

            byte [] next_refID = new byte[4];
            din.read(next_refID);
            int converted_next_refID = bigToLittleEndian(byteArrayToInt(next_refID,0));
            //System.out.println(">next_refID:"+converted_next_refID);
            
            byte [] next_pos = new byte[4];
            din.read(next_pos);
            int converted_next_pos = bigToLittleEndian(byteArrayToInt(next_pos,0));
            //System.out.println(">next_pos:"+converted_next_pos);
            
            byte [] tlen = new byte[4];
            din.read(tlen);
            int converted_tlen = bigToLittleEndian(byteArrayToInt(tlen,0));
            //System.out.println(">tlen:"+converted_tlen);
			*/

            //byte [] t = new byte[converted_block_size-8];
        	//din.read(t); 
	        byte [] t = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, (pipe_buffer_start+converted_block_size-8));
	        pipe_buffer_start += converted_block_size-8;
        	//System.out.println(new String(t));
       
        	//readBGZFBlock(in,fos);
        	//if (din.available() == 0){
        	if (pipe_buffer_length == pipe_buffer_start) {
        		//System.out.println("+--------------------------");
        		previous_bam_length = bam_length;
        		bgzf_block_size = readBGZFBlock(in,pipe_buffer_start,pipe_buffer_length);
        		bgzf_position += bgzf_block_size;
			flag = 1;
        	}

            //if (first_seq_pos - get_last_seq_pos > 200){
            if ((first_seq_pos - last_seq_pos > 50)&&(previous_converted_refID == converted_refID)){
                if(bgzf_position - mark >= hdfs_block_size){
                        if (flag == 1){

                        }else{
                                part_next_size = (int)(bam_position - previous_bam_length)-converted_block_size-4;
                                System.out.println(previous_bgzf_position+"\t"+part_previous_size+"\t"+(bgzf_position-bgzf_block_size)+"\t"+part_next_size);
splits.add(new FileSplit(new Path(new String(inputBamFile.toString()+"#"+(part_previous_size)+"#"+(part_next_size))), previous_bgzf_position, (bgzf_position-bgzf_block_size)-previous_bgzf_position, new String[]{}));
                                previous_bgzf_position = bgzf_position - bgzf_block_size;
                                part_previous_size = (int)(bam_length - bam_position)+converted_block_size+4;
                                mark = bgzf_position;
//System.out.println(first_seq_pos+"+++++++++++");
                        }
                }
            }
            last_seq_pos = first_seq_pos;
            previous_converted_refID = converted_refID;
            
        }	try{
		if (in != null){
		in.close();
		}
		}catch(Exception excp){}
		part_next_size = (int)(bam_position - previous_bam_length);
		System.out.println(previous_bgzf_position+"\t"+part_previous_size+"\t"+(bgzf_position-bgzf_block_size)+"\t"+part_next_size);
splits.add(new FileSplit(new Path(new String(inputBamFile.toString()+"#"+(part_previous_size)+"#"+part_next_size)), previous_bgzf_position, (bgzf_position-bgzf_block_size)-previous_bgzf_position, new String[]{}));
		return splits;
    }
	
	public static int readBGZFBlock(FSDataInputStream in, int buffer_position, int buffer_length){
		int bsize = 0;
		int total_len = 0;
		pipe_buffer_start = 0;
		try{
byte [] avail = new byte[1];
long begin = 0;
//while (in.read(begin, avail, 0, 1) > 0){
		//byte [] ID1 = new byte[1];
		//in.readFully(ID1,0,1);
		//System.out.println(in.getPos());
		//System.out.println(">ID1:"+new String(ID1));
/*	
	    	//byte ID1 = in.readByte();
	        //System.out.println(">ID1:"+new Short((short) (ID1 & 0x00ff)).toString()+"<");
	        //System.out.println("****************************");
	        byte ID2 = in.readByte();
	        //System.out.println(">ID2:"+new Short((short) (ID2 & 0x00ff)).toString()+"<"); 
	        
	        byte CM = in.readByte();
	        //System.out.println(">CM:"+new Short((short) (CM & 0x00ff)).toString()+"<"); 
	        
	        byte FLG = in.readByte();
	        //System.out.println(">FLG:"+new Short((short) (FLG & 0x00ff)).toString()+"<");
	         
	        int MTIME = in.readInt();
	        //int converted_MTIME = bigToLittleEndian(MTIME);
	        //System.out.println(">MTIME:"+new Long((long) (converted_MTIME & 0x00000000ffffffffl)).toString()+"<");
	        
	        byte XFL = in.readByte();
	        //System.out.println(">XFL:"+new Short((short) (XFL & 0x00ff)).toString()+"<");
	        
	        byte OS = in.readByte();
	        //System.out.println(">OS:"+new Short((short) (OS & 0x00ff)).toString()+"<");
*/	        
		byte [] head = new byte[10];
		//in.read(head);
		in.readFully(head);
		
	        //short XLEN = in.readShort();
		byte [] XLEN = new byte[2];
		in.readFully(XLEN);
	        short converted_XLEN = bigToLittleEndianShort(byteArrayToShort(XLEN,0));
	        int xlen = (int) converted_XLEN & 0x0000ffff;
	        //System.out.println(">XLEN:"+xlen+"<");
	        
	        /*
	        byte SI1 = in.readByte();
	        //System.out.println(">SI1:"+new Short((short) (SI1 & 0x00ff)).toString()+"<");
	        
	        byte SI2 = in.readByte();
	        //System.out.println(">SI2:"+new Short((short) (SI2 & 0x00ff)).toString()+"<");
	   		
	        short SLEN = in.readShort();
	        //short converted_SLEN = bigToLittleEndianShort(SLEN);
	        //System.out.println(">SLEN:"+new Integer((int) (converted_SLEN & 0x0000ffff)).toString()+"<");
			*/
	        byte [] subfield = new byte[4];
	        //in.read(subfield);
	        in.readFully(subfield);

	        //short BSIZE = in.readShort();
		byte [] BSIZE = new byte[2];
		in.readFully(BSIZE);
	        short converted_BSIZE = bigToLittleEndianShort(byteArrayToShort(BSIZE,0));
	        bsize = (int) (converted_BSIZE & 0x0000ffff);
	        //System.out.println(">BSIZE:"+bsize+"<");
//begin += bsize;	
//System.out.println("after bsize: "+in.getPos());
	        byte [] CDATA = new byte[bsize-xlen-19];
	        //int r = in.read(CDATA);
		in.readFully(CDATA);
	
	        //process the remaining zip metadata
	        //int CRC32 = in.readInt();
		byte [] CRC32 = new byte[4];
		in.readFully(CRC32);
	        //int converted_CRC32 = bigToLittleEndian(CRC32);
	        //System.out.println(">CRC32:"+new Long((long) (converted_CRC32 & 0x00000000ffffffffl)).toString()+"<");
	        
	        //int ISIZE = in.readInt();
		byte [] ISIZE = new byte[4];
		in.readFully(ISIZE);
	        int converted_ISIZE = bigToLittleEndian(byteArrayToInt(ISIZE,0));
	        //System.out.println(">ISIZE:"+new Long((long) (converted_ISIZE & 0x00000000ffffffffl)).toString()+"<");
//System.out.println("after isize: "+in.getPos());
	
	        //unzip compressed contents using inflate method
	        Inflater decompresser = new Inflater(true);//must use true here, since by default BAM do not include the zlib header
	        decompresser.setInput(CDATA);
	        
	        byte[] content = new byte[converted_ISIZE];
	        
	        if (buffer_position == buffer_length) {
	        	pipe_buffer_length = decompresser.inflate(content);
	        	bam_length += pipe_buffer_length;
	        	pipe_buffer = content;
	        }else{
	        	int j = 0;
	        	pipe_buffer_length = decompresser.inflate(content);
	        	bam_length += pipe_buffer_length;
	        	byte [] concatenate = new byte[pipe_buffer_length + buffer_length - buffer_position];
	        	for (int i=buffer_position;i<buffer_length;i++){
	        		concatenate[j] = pipe_buffer[i];
	        		j++;
	        	}

	        	for (int i=0;i<pipe_buffer_length;i++){
	        		concatenate[j] = content[i];
	        		j++;
	        	}
	        	pipe_buffer = concatenate;
	        	pipe_buffer_length = pipe_buffer_length + buffer_length - buffer_position;
	        }
	        decompresser.end();
//}	
		}
		catch(Exception e){}
		return bsize+1;//return bgzf block size, which is equal to bsize+1
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

}




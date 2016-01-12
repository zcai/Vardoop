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
public class CreateIntervalList{
	private byte [] pipe_buffer = null;
	private int pipe_buffer_length = 0;
	private long bam_position = 0;
	private long bam_length = 0;
	private int pipe_buffer_start = 0;
	//current position in the original BAM file
	private long bgzf_position = 0;
	//bgzf position of the current bgzf block
	private long previous_bgzf_position = 0;
	public int [] createStart(String input_bam_file, long block_start,long block_length,Configuration job) {
		int [] return_values = new int[900];//each split may span two or more chromosomes, set it to 300 at most
		int element = 0;//keep track of how many chromosomes a split spans

		bgzf_position = block_start;
		long block_end = block_start + block_length;

		FSDataInputStream in = null;

		long input_bam_file_length = 0;
		try{
			Path file = new Path(input_bam_file);
			FileSystem fs = file.getFileSystem(job);
			in = fs.open(file);
			input_bam_file_length = fs.getLength(file);
		}catch(Exception c){}

		long previous_bam_length = 0;
		
		int bgzf_block_size = 0;
		int previous_converted_refID = -1;
		int previous_converted_pos = 0;


		int first_seq_pos = 0;
		int last_seq_pos = 0;

		int MAX_SEQ_LEN = 1000;
		int MIN_SEQ_LEN = 30;
		//begin to read bam header
		//if (pipe_buffer_length == pipe_buffer_start) {
			bgzf_block_size = readBGZFBlock(in,pipe_buffer_start,pipe_buffer_length,bgzf_position);
			bgzf_position += bgzf_block_size;
		//}
		//System.out.println("pipe_buffer_length: "+pipe_buffer_length);
		int current_pipe_buffer_start = 0;

		//guess bam alignment section
		int check_first_split = 0;//check if it is the first split with bam head
		int first_split = 0;
		while (true){
			if (check_first_split == 0){
				check_first_split = 1;
				byte [] bam_magic = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+3);
				if ((new String((new String(bam_magic)).toCharArray())).equals("BAM")){
					System.out.println("BAM magic found");
					pipe_buffer_start += 4;
					bam_position += 4;
					
					//byte [] l_text = new byte[4];
					//din.read(l_text);
					byte [] l_text = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
					pipe_buffer_start += 4;
					int converted_l_text = bigToLittleEndian(byteArrayToInt(l_text,0));
					bam_position += 4;
					
					//byte [] text = new byte[converted_l_text];
					//din.read(text);
					byte [] text = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+converted_l_text);
					pipe_buffer_start += converted_l_text;
					bam_position += converted_l_text;
						   
					//byte [] n_ref = new byte[4];  
					//din.read(n_ref);
					byte [] n_ref = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
					pipe_buffer_start += 4;
					int converted_n_ref = bigToLittleEndian(byteArrayToInt(n_ref,0));
					bam_position += 4;
					
					for (int i=0;i<converted_n_ref;i++){
						//byte [] l_name = new byte[4];
						//din.read(l_name);
						byte [] l_name = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
						pipe_buffer_start += 4;
						int converted_l_name = bigToLittleEndian(byteArrayToInt(l_name,0));
						bam_position += 4;

						//byte [] name = new byte[converted_l_name];
						//din.read(name);
						byte [] name = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+converted_l_name);
						pipe_buffer_start += converted_l_name;
						bam_position += converted_l_name;
						
						//byte [] l_ref = new byte[4];
						//din.read(l_ref);
						byte [] l_ref = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
						pipe_buffer_start += 4;
						int converted_l_ref = bigToLittleEndian(byteArrayToInt(l_ref,0));
						bam_position += 4;
					}
				}
			}
			if (bam_position + 4 >= bam_length - 1){//
				bgzf_block_size = readBGZFBlock(in,pipe_buffer_start,pipe_buffer_length,bgzf_position);
				bgzf_position += bgzf_block_size;
			}

			current_pipe_buffer_start = pipe_buffer_start;
			byte [] bam_block_size = Arrays.copyOfRange(pipe_buffer, current_pipe_buffer_start, current_pipe_buffer_start+4);
			int converted_bam_block_size = bigToLittleEndian(byteArrayToInt(bam_block_size,0));
			current_pipe_buffer_start += 4;
			if (converted_bam_block_size < 0){
				bam_position += 1;
				pipe_buffer_start++;
				continue;
			}

			byte [] refID = Arrays.copyOfRange(pipe_buffer, current_pipe_buffer_start, current_pipe_buffer_start+4);
			int converted_refID = bigToLittleEndian(byteArrayToInt(refID,0));
			current_pipe_buffer_start += 4;
			if ((converted_refID < -1)||(converted_refID > 100)){
				bam_position += 1;
				pipe_buffer_start++;
				continue;
			}

			byte [] pos = Arrays.copyOfRange(pipe_buffer, current_pipe_buffer_start, current_pipe_buffer_start+4);
			int converted_pos = bigToLittleEndian(byteArrayToInt(pos,0));
			current_pipe_buffer_start += 4;
			if (converted_pos < -1){
				bam_position += 1;
				pipe_buffer_start++;
				continue;
			}else{
				//System.out.println(">>>pos: "+converted_pos);
			}

			byte [] l_read_name = Arrays.copyOfRange(pipe_buffer, current_pipe_buffer_start, current_pipe_buffer_start+1);
			int converted_l_read_name = (int) l_read_name[0];
			current_pipe_buffer_start += 1;

			//skip bin_mq_nl the first 3 bytes, the fourth byte is l_read_name
			current_pipe_buffer_start += 3;

			byte [] n_cigar_op = Arrays.copyOfRange(pipe_buffer, current_pipe_buffer_start, current_pipe_buffer_start+2);
			int converted_n_cigar_op = bigToLittleEndianShort(byteArrayToShort(n_cigar_op,0));
			current_pipe_buffer_start += 2;

			//skip flag
			current_pipe_buffer_start += 2;

			byte [] l_seq = Arrays.copyOfRange(pipe_buffer, current_pipe_buffer_start, current_pipe_buffer_start+4);
			int converted_l_seq = bigToLittleEndian(byteArrayToInt(l_seq,0));
			current_pipe_buffer_start += 4;
			if ((converted_l_seq < MIN_SEQ_LEN)||(converted_l_seq > MAX_SEQ_LEN)){
				bam_position += 1;
				pipe_buffer_start++;
				continue;
			}else{
				//System.out.println(">>>length: "+converted_l_seq);
			}
			byte [] next_refID = Arrays.copyOfRange(pipe_buffer, current_pipe_buffer_start, current_pipe_buffer_start+4);
			int converted_next_refID = bigToLittleEndian(byteArrayToInt(next_refID,0));
			current_pipe_buffer_start += 4;
			if ((converted_next_refID < -1)||(converted_next_refID > 100)){
				bam_position += 1;
				pipe_buffer_start++;
				continue;
			}else{
				//System.out.println(">>>next_refID: "+converted_next_refID);
			}

			byte [] next_pos = Arrays.copyOfRange(pipe_buffer, current_pipe_buffer_start, current_pipe_buffer_start+4);
			int converted_next_pos = bigToLittleEndian(byteArrayToInt(next_pos,0));
			current_pipe_buffer_start += 4;
			if (converted_next_pos < -1){
				bam_position += 1;
				pipe_buffer_start++;
				continue;
			}else{
				//System.out.println(">>>next_pos: "+converted_next_pos);
			}

			//skip tlen
			current_pipe_buffer_start += 4;

			//byte [] read_name = Arrays.copyOfRange(pipe_buffer, current_pipe_buffer_start, current_pipe_buffer_start+converted_l_read_name);
			//String str_read_name = (new String((new String(read_name)).toCharArray()));

			byte [] read_name_last_char = null;
			if ((current_pipe_buffer_start+converted_l_read_name >= 1)&&(current_pipe_buffer_start+converted_l_read_name <= pipe_buffer_length)){//should >= 1 ?????? need to work on it
				read_name_last_char = Arrays.copyOfRange(pipe_buffer, current_pipe_buffer_start+converted_l_read_name-1, current_pipe_buffer_start+converted_l_read_name);
			}else{
				bam_position += 1;
				pipe_buffer_start++;
				continue;
			}
			if (read_name_last_char[0] != '\0'){
				bam_position += 1;
				pipe_buffer_start++;
				continue;
			}else{
				//System.out.println(">>>read_name_last_char: "+read_name_last_char[0]);
			}

			//guess bam alignment record, need to work on it
			if ((converted_bam_block_size > 32+l_read_name.length+4*converted_n_cigar_op+(3*converted_l_seq+1)/2)){
				System.out.println("bam record found");
				//return refID and start pos
				//return_values[0] = converted_refID+1;
				//return_values[1] = converted_pos+1;
				return_values[element++] = converted_refID;
				return_values[element++] = converted_pos+1;
				previous_converted_refID = converted_refID;
				break;
			}else{
				bam_position += 1;
				pipe_buffer_start++;
			}
		}//while end

		//int converted_block_size = 0;
		int read_to_the_last_block_of_current_split = 0;
		int read_one_more_block = 0;
		int second_seq = 1;
		while (true){
			//a bgzf block span within the first 4 bytes of the next bgzf block
			if (pipe_buffer_start + 12 >= pipe_buffer_length - 1){
				previous_bam_length = bam_length;
				bgzf_block_size = readBGZFBlock(in,pipe_buffer_start,pipe_buffer_length,bgzf_position);
				bgzf_position += bgzf_block_size;
				if (bgzf_block_size == 28){//the final block is empty
					//return_values[2] = previous_converted_pos;//should plus the sequence length, need to work on it
					return_values[element++] = previous_converted_pos;//should plus the sequence length, need to work on it
					break;
				}
				if (read_to_the_last_block_of_current_split == 1){
					read_one_more_block = 1;
				}
			}

			byte [] block_size = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
			pipe_buffer_start += 4;
			int converted_block_size = bigToLittleEndian(byteArrayToInt(block_size,0));
			bam_position += 4;
			bam_position += converted_block_size;

			byte [] refID = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
			pipe_buffer_start += 4;
			int converted_refID = bigToLittleEndian(byteArrayToInt(refID,0));
			
			byte [] pos = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, pipe_buffer_start+4);
			pipe_buffer_start += 4;
			int converted_pos = bigToLittleEndian(byteArrayToInt(pos,0));
			
			if (pipe_buffer_start + converted_block_size-8 >= pipe_buffer_length - 1){
				bgzf_block_size = readBGZFBlock(in,pipe_buffer_start,pipe_buffer_length,bgzf_position);
				bgzf_position += bgzf_block_size;
				if (bgzf_block_size == 28){//the final block is empty
					//return_values[2] = previous_converted_pos;//should plus the sequence length, need to work on it
					return_values[element++] = previous_converted_pos;//should plus the sequence length, need to work on it
					break;
				}
				if (read_to_the_last_block_of_current_split == 1){
					read_one_more_block = 1;
				}
			}
			byte [] t = Arrays.copyOfRange(pipe_buffer, pipe_buffer_start, (pipe_buffer_start+converted_block_size-8));
			pipe_buffer_start += converted_block_size-8;

			if (converted_refID == -1){
				continue;
			}

			//if ((converted_refID != previous_converted_refID)&&(previous_converted_refID != -1)){//not working
			if ((converted_refID != previous_converted_refID)){//a split spans multiple chromosomes
				//return_values[2] = previous_converted_pos;
				return_values[element++] = previous_converted_pos + 50;//should be plus read length or to the end of the chromosome

				//begin another interval list info
				return_values[element++] = converted_refID;
				return_values[element++] = converted_pos+1;
				//break;
			}

			if (bgzf_position >= block_end){
				read_to_the_last_block_of_current_split = 1;
			}

			if (second_seq == 2){//begin from the second alignment record of a bgzf block, the first one may span two bgzf blocks
				//return_values[2] = converted_pos;
				return_values[element++] = converted_pos;
				break;
			}

			if (read_one_more_block == 1){//
				second_seq = 2;
			}


			previous_converted_refID = converted_refID;
			previous_converted_pos = converted_pos;

		}//end while
		
		try{
			if (in != null){
				in.close();
			}
		}catch(Exception excp){}
		
		return return_values;
	}
	
	private int readBGZFBlock(FSDataInputStream in, int buffer_position, int buffer_length,long split_start){
		//System.out.println(buffer_position+" "+buffer_length+" "+split_start);
		long bgzf_current_pos = 0;
		pipe_buffer_start = 0;
		int bsize = 0;
		try{
			while (true){
				bgzf_current_pos = split_start;
				byte [] bgzf_magic = new byte[4];
				in.readFully(bgzf_current_pos,bgzf_magic);
				int int_bgzf_magic = bigToLittleEndian(byteArrayToInt(bgzf_magic,0));
				bgzf_current_pos += 4;

				if (int_bgzf_magic == 0x04088b1f){
					//System.out.println("bgzf block found");
					break;
				}else{
					bgzf_position += 1;//set it to a bgzf boundary
					split_start += 1;
				}
			}//while end

			/*
			byte [] other = new byte[10];
			in.readFully(other);
			fos.write(other);
			*/
			
			split_start += 10;

			byte [] XLEN = new byte[2];
			in.readFully(split_start,XLEN);
			//fos.write(XLEN);
			short converted_XLEN = bigToLittleEndianShort(byteArrayToShort(XLEN,0));
			int xlen = (int) converted_XLEN & 0x0000ffff;
			
			
			/*
			byte [] sub = new byte[4];
			//in.read(sub);
			in.readFully(sub);
			fos.write(sub);
			*/
		
			split_start += 6;

			byte [] BSIZE = new byte[2];
			in.readFully(split_start,BSIZE);
			//in.readFully(BSIZE);
			//fos.write(BSIZE);
			short converted_BSIZE = bigToLittleEndianShort(byteArrayToShort(BSIZE,0));
			bsize = (int) (converted_BSIZE & 0x0000ffff);
			split_start += 2;

			//process compressed contents
			byte [] CDATA = new byte[bsize-xlen-19];
			//int r = in.read(CDATA);
			in.readFully(split_start,CDATA);
			//fos.write(CDATA);
			split_start += bsize-xlen-19;

			//process the remaining zip metadata
			byte [] CRC32 = new byte[4];
			//in.read(CRC32);
			in.readFully(split_start,CRC32);
			//fos.write(CRC32);
			//int converted_CRC32 = bigToLittleEndian(CRC32);
			split_start += 4;

			byte [] ISIZE = new byte[4];
			//in.read(ISIZE);
			in.readFully(split_start,ISIZE);
			//fos.write(ISIZE);
			int converted_ISIZE = bigToLittleEndian(byteArrayToInt(ISIZE,0));
			split_start += 4;

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
		}catch(Exception e){}

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




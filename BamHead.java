import java.util.*;

class BamHead {
	private int length;
	private HashMap ref_name;//reference sequence names in Bam head, such as chr1,chr2,chr3... It maps reference sequence id to name.

	HashMap getRef(){
		return ref_name;
	}
	void setRef(HashMap ref_name){
		this.ref_name = ref_name;
	}
	int getLength(){
		return length;
	}
	void setLength(int length){
		this.length = length;
	}
}

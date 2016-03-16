package minidb.storagemanager;

public abstract class AbstractDBFile {
	
	private String fileName;
	private int totalNumOfBlocks;
	private int curBlockPos;
	
	
	
	public abstract int getFileSize(); //returns file size in bytes
	
	
}

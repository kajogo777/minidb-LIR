package minidb.storagemanager;

public abstract class AbstractDBFile {
	
	protected String fileName;
	protected int totalNumOfBlocks;
	protected int curBlockPos;
	
	
	public abstract int getFileSize(); //returns file size in bytes

	
}

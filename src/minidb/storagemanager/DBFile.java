package minidb.storagemanager;

import java.util.ArrayList;

public class DBFile extends AbstractDBFile{
	
	private int blockSize;

	public DBFile(String fileName, int numBlocks, int blcks){
		blockSize = blcks;
		totalNumOfBlocks = numBlocks;
		curBlockPos = 1;
		fileName = fileName;
	}
			
	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	@Override
	public int getFileSize() {
		return getBlockSize() * totalNumOfBlocks;
	}
	
	

}

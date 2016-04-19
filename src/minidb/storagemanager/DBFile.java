package minidb.storagemanager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class DBFile extends AbstractDBFile{
	
	private int blockSize;
	private RandomAccessFile onDiskFile;
	private static String myProjPath = System.getProperty("user.dir") + "/";

	public DBFile(String fileName, int numBlocks, int blcks) throws IOException{
		blockSize = blcks;
		totalNumOfBlocks = numBlocks;
		curBlockPos = 0;
		this.fileName = fileName;
		onDiskFile = new RandomAccessFile(myProjPath + fileName, "rw");
		onDiskFile.seek(StorageManager.headerSize);
	}
	
	public int getTotalNumberOfBlocks()
	{
		return totalNumOfBlocks;
	}
	
	public RandomAccessFile getOnDiskFile() {
		return onDiskFile;
	}

	public void setOnDiskFile(RandomAccessFile onDiskFile) {
		this.onDiskFile = onDiskFile;
	}

	public void changeOnDiskPointer(int block) throws IOException{
		getOnDiskFile().seek( StorageManager.headerSize + (block) * getBlockSize());
		curBlockPos = block;
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

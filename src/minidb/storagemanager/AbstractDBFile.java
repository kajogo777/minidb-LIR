package minidb.storagemanager;

public abstract class AbstractDBFile {
	
	private String fileName;
	private int totalNumOfBlocks;
	private int curBlockPos;
	
	
	public String getFileName() {
		return fileName;
	}



	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	
	public abstract int getFileSize(); //returns file size in bytes



	public int getTotalNumOfBlocks() {
		return totalNumOfBlocks;
	}



	public void setTotalNumOfBlocks(int totalNumOfBlocks) {
		this.totalNumOfBlocks = totalNumOfBlocks;
	}



	public int getCurBlockPos() {
		return curBlockPos;
	}



	public void setCurBlockPos(int curBlockPos) {
		this.curBlockPos = curBlockPos;
	}
	
	
}

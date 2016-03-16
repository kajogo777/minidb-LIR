package minidb.storagemanager;

import java.util.ArrayList;

public class DBFile extends AbstractDBFile{
	
	private ArrayList<Block> blocks;
	
	public ArrayList<Block> getBlocks() {
		return blocks;
	}

	public void setBlocks(ArrayList<Block> blocks) {
		this.blocks = blocks;
	}

	public DBFile(String fileName, int numBlocks){
		blocks = new ArrayList<Block>(numBlocks);
		setTotalNumOfBlocks(numBlocks);
		setCurBlockPos(0);
		setFileName(fileName);
	}
			
	@Override
	public int getFileSize() {
		return blocks.get(0).getData().length * getTotalNumOfBlocks();
	}
	
	

}

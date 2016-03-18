package minidb.storagemanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class StorageManager implements IStorageManager{
	
	private final int headerSize = 4000;
	private int blockSize;
	private String myProjPath = System.getProperty("user.dir") + "/";

	
	public StorageManager()
	{
		init();
	}
	
	@Override
	public void init() {	
		BufferedReader reader;
		try {
			reader = Files.newBufferedReader(Paths.get(myProjPath + "conf/minidb.config"));
			blockSize = 4096;//Integer.parseInt(reader.readLine().split("=")[1]);
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public AbstractDBFile createFile(String fileName) throws IOException {
		
        DBFile fileHandler = new DBFile(fileName, 1, blockSize);
        
        byte[] header = String.format("%d,%d",1,blockSize).getBytes();// num of blocks, blocksize
        
        Block headerBlk = new Block(headerSize);
        headerBlk.setData(header);
        
        Block emptyBlk = new Block(blockSize);

        RandomAccessFile file = new RandomAccessFile(myProjPath + fileName, "rw");	
        file.write(headerBlk.getData(), 0, headerSize);
		file.write(emptyBlk.getData(), 0, blockSize);
		file.close();
        
        return fileHandler;
	}

	@Override
	public AbstractDBFile openFile(String fileName) throws IOException {
		//Path filep = Paths.get(fileName);
		
		RandomAccessFile file = new RandomAccessFile(myProjPath +fileName, "rw");

		byte[] aByte = new byte[headerSize];
		
		file.read(aByte, 0, headerSize);
		file.close();
		
		String[] headerValues = (new String(aByte)).split(",");

		DBFile fileHandler = new DBFile(fileName, Integer.parseInt(headerValues[0]), Integer.parseInt(headerValues[1].split("\u0000")[0]));
		
		return fileHandler;
	}

	@Override
	public void closeFile(AbstractDBFile f) throws IOException {
			DBFile fh = (DBFile) f;
		
		    byte[] header = String.format("%d,%d",fh.totalNumOfBlocks,fh.getBlockSize()).getBytes();// num of blocks, blocksize
	        
	        Block headerBlk = new Block(headerSize);
	        headerBlk.setData(header);

	        RandomAccessFile file = new RandomAccessFile(myProjPath + fh.fileName, "rw");	
	        file.write(headerBlk.getData(), 0, headerSize);
			file.close();
	}

	@Override
	public void deleteFile(String fileName) throws IOException {
		File f = null;
	      boolean bool = false;
	      
	      try{
	         
	         f = new File(myProjPath +fileName);
	         
	         bool = f.delete();
	         
	         System.out.println("File deleted: "+bool);
	            
	      }catch(Exception e){
	         // if any error occurs
	         e.printStackTrace();
	      }
	}

	@Override
	public AbstractBlock readBlock(int blockNum, AbstractDBFile f) throws IOException {
		
		DBFile fh = (DBFile) f;
		
		RandomAccessFile file = new RandomAccessFile(myProjPath +f.fileName, "rw");
		
		Block blk = new Block(fh.getBlockSize());

		byte[] aByte = new byte[fh.getBlockSize()];
		
		file.seek(headerSize + (blockNum) * fh.getBlockSize());
		file.read(aByte, 0, fh.getBlockSize());
		file.close();
		blk.setData(aByte);
		f.curBlockPos = blockNum;
		
		return blk;
	}

	@Override
	public int getBlockPos(AbstractDBFile f) throws IOException {
		return f.curBlockPos;
	}

	@Override
	public AbstractBlock readFirstBlock(AbstractDBFile f) throws IOException {
		return readBlock(0, f);
	}

	@Override
	public AbstractBlock readPreviousBlock(AbstractDBFile f) throws IOException {
		DBFile fh = (DBFile) f;
		return readBlock(fh.curBlockPos -1, f);
	}

	@Override
	public AbstractBlock readCurrentBlock(AbstractDBFile f) throws IOException {
		DBFile fh = (DBFile) f;
		return readBlock(fh.curBlockPos, f);
	}

	@Override
	public AbstractBlock readNextBlock(AbstractDBFile f) throws IOException {
		DBFile fh = (DBFile) f;
		return readBlock(fh.curBlockPos +1, f);
	}

	@Override
	public AbstractBlock readLastBlock(AbstractDBFile f) throws IOException {
		return readBlock(f.totalNumOfBlocks -1, f);
	}

	@Override
	public void writeBlock(int blockNum, AbstractDBFile f, AbstractBlock b) throws IOException {
		DBFile fh = (DBFile) f;
		Block blk = (Block) b;
		
		if(blockNum >= fh.totalNumOfBlocks)
			fh.totalNumOfBlocks++;
		
		RandomAccessFile file = new RandomAccessFile(myProjPath +f.fileName, "rw");
		
		file.seek(headerSize + (blockNum)* fh.getBlockSize() );
		file.write(blk.getData(), 0, fh.getBlockSize());
		file.close();
		f.curBlockPos = blockNum + 1;
	}

	@Override
	public void writeCurrentBlock(AbstractDBFile f, AbstractBlock b) throws IOException {
		writeBlock(f.curBlockPos, f, b);
	}

	@Override
	public void appendEmptyBlock(AbstractDBFile f) throws IOException {
		DBFile fh = (DBFile) f;
		Block blk = new Block(fh.getBlockSize());
		fh.totalNumOfBlocks = fh.totalNumOfBlocks +1;
		
		writeBlock(fh.totalNumOfBlocks -1, f, blk);
	}
	
	

}

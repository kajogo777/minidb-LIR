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

	@Override
	public void init() {	
		BufferedReader reader;
		try {
			reader = Files.newBufferedReader(Paths.get("./minidb-LIR/conf/minidb.config"));
			blockSize = Integer.parseInt(reader.readLine().split("=")[1]);
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public AbstractDBFile createFile(String fileName) throws IOException {
        
        DBFile fileHandler = new DBFile(fileName, 1, blockSize);
        
        byte[] header = String.format("%d,%d",1,blockSize).getBytes(Charset.forName("UTF-8"));// num of blocks, blocksize
        
        Block headerBlk = new Block(headerSize);
        headerBlk.setData(header);
        
        Block emptyBlk = new Block(blockSize);
        
        Path file = Paths.get(fileHandler.fileName);
        Files.write(file, headerBlk.getData());
        Files.write(file, emptyBlk.getData());      
        
        return fileHandler;
	}

	@Override
	public AbstractDBFile openFile(String fileName) throws IOException {
		//Path filep = Paths.get(fileName);
		
		RandomAccessFile file = new RandomAccessFile(fileName, "rw");

		byte[] aByte = new byte[headerSize];
		
		file.read(aByte, 0, headerSize);
		file.close();
		
		String[] headerValues = (new String(aByte, "UTF-8")).split(",");
		
		DBFile fileHandler = new DBFile(fileName, Integer.parseInt(headerValues[0]), Integer.parseInt(headerValues[1]));
		
		return fileHandler;
	}

	@Override
	public void closeFile(AbstractDBFile f) throws IOException {

	}

	@Override
	public void deleteFile(String fileName) throws IOException {
		File f = null;
	      boolean bool = false;
	      
	      try{
	         
	         f = new File(fileName);
	         
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
		
		RandomAccessFile file = new RandomAccessFile(f.fileName, "rw");
		
		Block blk = new Block(fh.getBlockSize());

		byte[] aByte = new byte[fh.getBlockSize()];
		
		file.seek(headerSize + (blockNum-1) * fh.getBlockSize());
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
		//return readBlock(1, f);
		DBFile fh = (DBFile) f;
		
		RandomAccessFile file = new RandomAccessFile(f.fileName, "rw");
		
		Block blk = new Block(fh.getBlockSize());

		byte[] aByte = new byte[fh.getBlockSize()];
		
		file.seek(headerSize);
		file.read(aByte, 0, fh.getBlockSize());
		file.close();
		blk.setData(aByte);
		f.curBlockPos = 1;
		
		return blk;
	}

	@Override
	public AbstractBlock readPreviousBlock(AbstractDBFile f) throws IOException {
		DBFile fh = (DBFile) f;
		
		RandomAccessFile file = new RandomAccessFile(f.fileName, "rw");
		
		Block blk = new Block(fh.getBlockSize());

		byte[] aByte = new byte[fh.getBlockSize()];
		
		file.seek(headerSize + (f.curBlockPos -2)* fh.getBlockSize() );
		file.read(aByte, 0, fh.getBlockSize());
		file.close();
		blk.setData(aByte);
		f.curBlockPos = f.curBlockPos -1;
		
		return blk;
	}

	@Override
	public AbstractBlock readCurrentBlock(AbstractDBFile f) throws IOException {
		DBFile fh = (DBFile) f;
		
		RandomAccessFile file = new RandomAccessFile(f.fileName, "rw");
		
		Block blk = new Block(fh.getBlockSize());

		byte[] aByte = new byte[fh.getBlockSize()];
		
		file.seek(headerSize + (f.curBlockPos -1)* fh.getBlockSize() );
		file.read(aByte, 0, fh.getBlockSize());
		file.close();
		blk.setData(aByte);
		f.curBlockPos = f.curBlockPos;
		
		return blk;
	}

	@Override
	public AbstractBlock readNextBlock(AbstractDBFile f) throws IOException {
		DBFile fh = (DBFile) f;
		
		RandomAccessFile file = new RandomAccessFile(f.fileName, "rw");
		
		Block blk = new Block(fh.getBlockSize());

		byte[] aByte = new byte[fh.getBlockSize()];
		
		file.seek(headerSize + (f.curBlockPos)* fh.getBlockSize() );
		file.read(aByte, 0, fh.getBlockSize());
		file.close();
		blk.setData(aByte);
		f.curBlockPos = f.curBlockPos +1;
		
		return blk;
	}

	@Override
	public AbstractBlock readLastBlock(AbstractDBFile f) throws IOException {
		DBFile fh = (DBFile) f;
		
		RandomAccessFile file = new RandomAccessFile(f.fileName, "rw");
		
		Block blk = new Block(fh.getBlockSize());

		byte[] aByte = new byte[fh.getBlockSize()];
		
		file.seek(headerSize + (f.totalNumOfBlocks -1)* fh.getBlockSize() );
		file.read(aByte, 0, fh.getBlockSize());
		file.close();
		blk.setData(aByte);
		f.curBlockPos = f.totalNumOfBlocks;
		
		return blk;
	}

	@Override
	public void writeBlock(int blockNum, AbstractDBFile f, AbstractBlock b) throws IOException {
		DBFile fh = (DBFile) f;
		Block blk = (Block) b;
		
		RandomAccessFile file = new RandomAccessFile(f.fileName, "rw");
		
		file.seek(headerSize + (blockNum -1)* fh.getBlockSize() );
		file.write(blk.getData(), 0, fh.getBlockSize());
		file.close();
		f.curBlockPos = blockNum;
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
		
		writeBlock(fh.totalNumOfBlocks, f, blk);
	}
	
	

}

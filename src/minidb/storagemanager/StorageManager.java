package minidb.storagemanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class StorageManager implements IStorageManager{

	@Override
	public void init() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public AbstractDBFile createFile(String fileName) throws IOException {
		
		BufferedReader reader = Files.newBufferedReader(Paths.get("./minidb-LIR/conf/minidb.config"));
		int blockSize = Integer.parseInt(reader.readLine().split("=")[1]);
		reader.close();
        
        DBFile fileHandler = new DBFile(fileName, 1, blockSize);
        
        byte[] header = String.format("numOfBlocks=%d,blockSize=%d",1,blockSize).getBytes(Charset.forName("UTF-8"));
        
        Block headerBlk = new Block(blockSize);
        headerBlk.setData(header);
        
        Path file = Paths.get(fileHandler.getFileName());
        Files.write(file, headerBlk.getData());
        
        return fileHandler;
	}

	@Override
	public AbstractDBFile openFile(String fileName) throws IOException {
		
		return null;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getBlockPos(AbstractDBFile f) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public AbstractBlock readFirstBlock(AbstractDBFile f) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractBlock readPreviousBlock(AbstractDBFile f) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractBlock readCurrentBlock(AbstractDBFile f) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractBlock readNextBlock(AbstractDBFile f) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractBlock readLastBlock(AbstractDBFile f) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void writeBlock(int blockNum, AbstractDBFile f, AbstractBlock b) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeCurrentBlock(AbstractDBFile f, AbstractBlock b) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void appendEmptyBlock(AbstractDBFile f) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	

}

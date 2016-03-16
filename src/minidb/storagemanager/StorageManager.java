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
        
        DBFile fileHandler = new DBFile(fileName, 1);
        Block blk = new Block(blockSize);
        fileHandler.getBlocks().add(blk);
        
        return fileHandler;
	}

	@Override
	public AbstractDBFile openFile(String fileName) throws IOException {
		Path file = Paths.get(fileName);
		byte[] data = Files.readAllBytes(file);
		
		BufferedReader reader = Files.newBufferedReader(Paths.get("./minidb-LIR/conf/minidb.config"));
		int blockSize = Integer.parseInt(reader.readLine().split("=")[1]);
		reader.close();
		
		String dlr = "$";
		byte bdlr = dlr.getBytes("UTF-8")[0];
		boolean founddlr = false;
		String header = "";
		//String[] info;
		
		DBFile fileHandler;
		
		
		for(byte b : data)
		{
			if(!founddlr)
			{
				if(b ==  bdlr)
				{
					founddlr = true;
					//info = header.split(",");
					fileHandler = new DBFile(fileName, Integer.parseInt(header));
				}else{
					byte[] chr = { b };
					header += new String( chr, Charset.forName("UTF-8"));
				}
			}else{
				
			}
		}
		
		
		return null;
	}

	@Override
	public void closeFile(AbstractDBFile f) throws IOException {
		// TODO check if file exist
		Path file = Paths.get(f.getFileName());

		if(! new File(f.getFileName()).exists())
			Files.createFile(file);
		
		String header = f.getTotalNumOfBlocks() + "," + f.getFileSize()/f.getTotalNumOfBlocks() + "$";
		
		byte[] allData = header.getBytes(Charset.forName("UTF-8"));
	
		for(Block b : ((DBFile)f).getBlocks()){
			byte[] tempData = new byte[allData.length + b.getData().length];
			System.arraycopy(allData, 0, tempData, 0, allData.length);
			System.arraycopy(b.getData(), 0, tempData, allData.length, b.getData().length);
			allData = tempData;
		}
		
		Files.write(file, allData);	
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

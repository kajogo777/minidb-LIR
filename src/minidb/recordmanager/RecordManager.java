package minidb.recordmanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;

import minidb.storagemanager.Block;
import minidb.storagemanager.DBFile;
import minidb.storagemanager.StorageManager;

public class RecordManager implements IRecordManager{

	private MetaData openTable(String tableName){
		StorageManager sm = new StorageManager();
		DBFile dbf;
		try {
			dbf = (DBFile) sm.openFile(tableName);
			Block metaBlock = (Block) sm.readFirstBlock(dbf);
			String[] metaInfo = (new String(metaBlock.getData())).split("/");
			MetaData mt = new MetaData(metaInfo.length-1);
			Character c = new Character((char) metaBlock.getData()[0]);
			for(int i = 0; i < metaInfo.length-1; i++)
			{
				String[] col = metaInfo[i].split("$");
				mt.columnNames[i] = col[0];
				mt.dataTypes[i] = col[1];
				mt.isKey[i] = Boolean.valueOf(col[2]);
				mt.references[i] = col[3];
			}
			
			mt.setSlotSize();
			mt.setSlotsPerBlock(dbf.getBlockSize());
			
			byte[] tmpBytes = metaInfo[metaInfo.length-1].getBytes();
			
			int bytes = (int) Math.ceil(mt.slotsPerBlock/8.0);
			
			for(int j = 0; j < tmpBytes.length; j += bytes)
				mt.blocks.set(j, BitSet.valueOf(Arrays.copyOfRange(tmpBytes, j, bytes)));
			
			mt.dbFile = dbf;
			
			if(mt.dbFile.getFileSize()/mt.dbFile.getBlockSize() == 1)
			{
				sm.appendEmptyBlock(mt.dbFile);
				mt.addEmptyBlock();
			}
			
			return mt;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}	
	}
	
	private void closeTable(String tableName, MetaData mt)
	{
		StorageManager sm = new StorageManager();
		try {
			Block metaBlock = new Block();
			
			String metaInfo = "";
			
			for(int i = 0 ; i < mt.columnNames.length ; i++)
				metaInfo += String.format("%s$%s$%s$%s/", mt.columnNames[i], mt.dataTypes[i], mt.isKey[i], mt.references[i]);
			
			for(BitSet b : mt.blocks)
				metaInfo += new String(b.toByteArray());
			
			metaBlock.setData(metaInfo.getBytes());
			sm.writeBlock(0, mt.dbFile, metaBlock);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void createTable(String tableName, String[] columnNames, String[] dataTypes, boolean[] isKey,
			String[] references) throws AbstractRecordManagerException {
		// TODO Auto-generated method stub
		
		StorageManager storagemanager = new StorageManager();
		try {
			DBFile NewFile = (DBFile)storagemanager.createFile(tableName);
		 
			Block block = new Block(NewFile.getBlockSize());
			storagemanager.writeBlock(0, NewFile,block);
			
			String metaInfo = "";
			
			for (int i =0;i<columnNames.length;i++){
				metaInfo += String.format("%s$%s$%s$%s/", columnNames[i], dataTypes[i], isKey[i], references[i]);
			}	
			
			block.setData(metaInfo.getBytes());
			storagemanager.writeBlock(0, NewFile,block);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

	@Override
	public void dropTable(String tableName) throws AbstractRecordManagerException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public AbstractRecord getRecord(String tableName, RecordID rid)
			throws AbstractRecordManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractRecord[] getRecord(String tableName, String columnName, String dataType, String value) {
		// TODO Auto-generated method stub
		
		return null;
	}

	@Override
	public void deleteRecord(AbstractRecord r, String tableName) throws AbstractRecordManagerException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateRecord(AbstractRecord r, String tableName) throws AbstractRecordManagerException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insertRecord(AbstractRecord r, String tableName) throws AbstractRecordManagerException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createIndex(String tableName, String columnName) throws AbstractRecordManagerException {
		// TODO Auto-generated method stub
		
	}

}

class MetaData{
	public String[] columnNames;
	public String[] dataTypes;
	public boolean[] isKey;
	public String[] references;
	
	public ArrayList<BitSet> blocks;
	
	public int slotSize;
	public int slotsPerBlock;
	public DBFile dbFile;

	public MetaData(int colN)
	{
		columnNames = new String[colN];
		dataTypes = new String[colN];
		isKey = new boolean[colN];
		references = new String[colN];
	}
	
	private int getType(String type){
		int size = 0;
		switch(type){
			case "java.lang.Integer":
				size = 4;
				break;
			case "java.lang.Boolean":
				size = 1;
				break;
			case "java.util.Date":
				size = 8;
				break;
			case "java.lang.String":
				size = 100;
				break;
			default:
				//"java.lang.String:100"
				size = Integer.parseInt(type.split(":")[1]);
				break;
		}
		return size;
	}
	
	public void setSlotSize(){
		for(int i = 0; i < dataTypes.length; i++){
			slotSize += getType(dataTypes[i]);
		}
	}
	
	public void setSlotsPerBlock(int blockSize){
		slotsPerBlock = blockSize/slotSize;
	}
	
	public void addEmptyBlock(){
		BitSet b = new BitSet(slotsPerBlock);
		b.set(0, b.size());
		blocks.add(b);
	}
}

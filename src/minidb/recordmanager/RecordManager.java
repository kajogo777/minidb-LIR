package minidb.recordmanager;

import java.io.IOException;
import java.nio.ByteBuffer;
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
				mt.bitArray.set(j, BitSet.valueOf(Arrays.copyOfRange(tmpBytes, j, bytes)));
			
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
	
	private void saveTableMetaData(String tableName, MetaData mt)
	{
		StorageManager sm = new StorageManager();
		try {
			Block metaBlock = new Block();
			
			String metaInfo = "";
			
			for(int i = 0 ; i < mt.columnNames.length ; i++)
				metaInfo += String.format("%s$%s$%s$%s/", mt.columnNames[i], mt.dataTypes[i], mt.isKey[i], mt.references[i]);
			
			for(BitSet b : mt.bitArray)
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
		StorageManager sm = new StorageManager();
		try {
			sm.deleteFile(tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public AbstractRecord getRecord(String tableName, RecordID rid)
			throws AbstractRecordManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractRecord[] getRecord(String tableName, String columnName, String dataType, String value) {
		MetaData mt = openTable(tableName);
		
		
		return null;
	}

	@Override
	public void deleteRecord(AbstractRecord r, String tableName) throws AbstractRecordManagerException {
		MetaData mt = openTable(tableName);	
	    RecordID index = ((Record)r).getKey();			
		mt.clearSlot(index);
		saveTableMetaData(tableName, mt);
	}

		
	

	@Override
	public void updateRecord(AbstractRecord r, String tableName) throws AbstractRecordManagerException {
		 MetaData mt = openTable(tableName);	
	       RecordID index = ((Record)r).getKey() ;
	       StorageManager sm = new StorageManager();
	       try {
				Block b = (Block) sm.readBlock( index.getBlockNumber(), mt.dbFile);
				byte[] array = b.getData();
				int slotlocation = mt.slotSize * index.getSlotNumber();
				
				String[] types = r.getDataTypes();
				String[] values = r.getValues();
				for(int i = 0, bi = slotlocation; i < types.length; i++)
				{
					byte[] tmp = mt.getType(types[i], values[i]);
					for(int j = 0; j < tmp.length; j++, bi++)
						array[bi] = tmp[j];
				}
				
				b.setData(array);
				sm.writeBlock(index.getBlockNumber(), mt.dbFile, b);
				
	       } catch (IOException e) {
				e.printStackTrace();
			} 
	}

	@Override
	public void insertRecord(AbstractRecord r, String tableName) throws AbstractRecordManagerException {
       MetaData mt = openTable(tableName);	
       RecordID index = mt.getFreeSlot();
       StorageManager sm = new StorageManager();
       try {
			Block b = (Block) sm.readBlock( index.getBlockNumber(), mt.dbFile);
			byte[] array = b.getData();
			int slotlocation = mt.slotSize * index.getSlotNumber();
			
			String[] types = r.getDataTypes();
			String[] values = r.getValues();
			for(int i = 0, bi = slotlocation; i < types.length; i++)
			{
				byte[] tmp = mt.getType(types[i], values[i]);
				for(int j = 0; j < tmp.length; j++, bi++)
					array[bi] = tmp[j];
			}
			
			b.setData(array);
			sm.writeBlock(index.getBlockNumber(), mt.dbFile, b);
			mt.fillSlot(index);
			saveTableMetaData(tableName, mt);
		} catch (IOException e) {
			e.printStackTrace();
		} 
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
	
	public ArrayList<BitSet> bitArray;
	
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
	
	public byte[] getType(String type, String value)
	{
		ByteBuffer bf;
		switch(type){
			case "java.lang.Integer":
				bf = ByteBuffer.allocate(4);
				bf.putInt(Integer.parseInt(value));	
				break;
			case "java.lang.Boolean":
				bf = ByteBuffer.allocate(1);
				byte[] b = new byte[1];
				b[0] = (byte) (value.compareToIgnoreCase("true") == 0 ? 1 : 0);
				bf.put(b, 0, 1);
				break;
			case "java.util.Date":
				bf = ByteBuffer.allocate(8);
				bf.putLong(Long.parseLong(value));
				break;
			case "java.lang.String":
				bf = ByteBuffer.allocate(100);
				bf.put(value.getBytes());
				break;
			default:
				//"java.lang.String:100"
				int size = Integer.parseInt(type.split(":")[1]);
				bf = ByteBuffer.allocate(size);
				bf.put(value.getBytes());
				break;
		}
		return bf.array();
	}
	
	
	private int getTypeSize(String type){
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
			slotSize += getTypeSize(dataTypes[i]);
		}
	}
	
	public void setSlotsPerBlock(int blockSize){
		slotsPerBlock = blockSize/slotSize;
	}
	
	public void addEmptyBlock(){
		BitSet b = new BitSet(slotsPerBlock);
		b.set(0, b.size());
		bitArray.add(b);
	}
	
	public void fillSlot(RecordID ri)
	{
		bitArray.get(ri.getBlockNumber()).clear(ri.getSlotNumber());
	}
	
	public void clearSlot(RecordID ri)
	{
		bitArray.get(ri.getBlockNumber()).set(ri.getSlotNumber());
	}
	
	public RecordID getFreeSlot(){
		
		RecordID ri = new RecordID();
		boolean found = false;
		
		for(int i = 0; i < bitArray.size(); i++)
		{
			int slot = bitArray.get(i).nextSetBit(0);
			if(slot > -1)
			{
				found = true;
				ri.setBlockNumber(i+1);
				ri.setSlotNumber(slot);
				break;
			}
		}
		
		if(!found){
			StorageManager sm = new StorageManager();
			try {
				sm.appendEmptyBlock(dbFile);
				addEmptyBlock();
				ri.setBlockNumber(bitArray.size());
				ri.setSlotNumber(0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return ri;
	}
}

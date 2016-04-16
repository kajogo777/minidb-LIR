package minidb.recordmanager;

import java.io.IOException;
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
			mt.dbFile = dbf;
			return mt;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}	
	}
	
	@Override
	public void createTable(String tableName, String[] columnNames, String[] dataTypes, boolean[] isKey,
			String[] references) throws AbstractRecordManagerException {
		// TODO Auto-generated method stub
		
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
		MetaData mt = openTable(tableName);
		
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
	
	public BitSet[] blocks;
	
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
}

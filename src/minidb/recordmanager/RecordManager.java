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

	public static void main(String[] args) throws Exception
	{
		RecordManager rm = new RecordManager();
		
		//create
		String[] columnNames = {"id", "name", "age"};
		String[] dataTypes = {"java.lang.Integer", "java.lang.String:10", "java.lang.Integer"};
		boolean[] isKey = {true, false, false};
		String[] references = {"Null", "Null", "Null"};
		rm.createTable("Student", columnNames, dataTypes, isKey, references);
		
		
		//insert
		String[] mahmoudValues = {"3", "mahmoud", "20"};
		for(int i = 1; i <= 10; i++)
		{
			mahmoudValues[0] = "" + i;
			mahmoudValues[2] = "" + 20;
			Record r = new Record( columnNames, dataTypes, mahmoudValues, references, null);
			rm.insertRecord(r, "Student");
		}
		
		//update
		AbstractRecord[] results = rm.getRecord("Student", "age", "java.lang.Integer", "20");
		Record temp = (Record) results[3];
		temp.values[1] = "George";
		rm.updateRecord(temp, "Student");
		
		//delete
		temp = (Record) results[0];
		rm.deleteRecord(temp, "Student");
		
		//insert
		Record r = new Record( columnNames, dataTypes, mahmoudValues, references, null);
		rm.insertRecord(r, "Student");
		
		//get
		results = rm.getRecord("Student", "age", "java.lang.Integer", "20");
		for(AbstractRecord rec : results)
		{
			System.out.printf("< %s: %s, %s: %s, %s: %s >\n",
					rec.getColumnNames()[0],rec.getValues()[0],
					rec.getColumnNames()[2],rec.getValues()[2],
					rec.getColumnNames()[1],rec.getValues()[1]);		
		}
		
		//drop table
		rm.dropTable("Student");
	}

	private MetaData openTable(String tableName){
		StorageManager sm = new StorageManager();
		DBFile dbf;
		try {
			dbf = (DBFile) sm.openFile(tableName);
			Block metaBlock = (Block) sm.readFirstBlock(dbf);
			String[] metaInfo = (new String(metaBlock.getData())).split("/");
			MetaData mt = new MetaData(metaInfo.length-1);
			int bitMapOffset = metaInfo.length -1;
			for(int i = 0; i < metaInfo.length-1; i++)
			{
				bitMapOffset += metaInfo[i].length();
				
				String[] col = metaInfo[i].split(",");
				
				mt.columnNames[i] = col[0];
				mt.dataTypes[i] = col[1];
				mt.isKey[i] = Boolean.valueOf(col[2]);
				mt.references[i] = col[3];
			}
			
			mt.setSlotSize();
			mt.setSlotsPerBlock(dbf.getBlockSize());
			
			mt.dbFile = dbf;
			
			int bytesPerBitMap = ((mt.slotsPerBlock + 7) / 8);

			if(mt.dbFile.getTotalNumberOfBlocks() == 1)
			{
				sm.appendEmptyBlock(mt.dbFile);
				mt.addEmptyBlock();
			}else{
				byte[] bitMap = Arrays.copyOfRange(metaBlock.getData(), bitMapOffset, bitMapOffset+mt.dbFile.getTotalNumberOfBlocks() * ((mt.slotsPerBlock + 7) / 8));// ((mt.slotsPerBlock+31)/32)*4);
				
				for(int j = 0; j < mt.dbFile.getTotalNumberOfBlocks()-1; j++)
				{
					BitSet tmp = MetaData.toBitArray(Arrays.copyOfRange(bitMap, j*bytesPerBitMap, j*bytesPerBitMap+bytesPerBitMap));
					mt.bitArray.add(0, tmp);
				}
			}
			
			return mt;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}	
	}
	
	
	@Override
	public void createTable(String tableName, String[] columnNames, String[] dataTypes, boolean[] isKey,
			String[] references) throws AbstractRecordManagerException {
		
		StorageManager storagemanager = new StorageManager();
		try {
			DBFile NewFile = (DBFile)storagemanager.createFile(tableName);
		 
			Block block = new Block(NewFile.getBlockSize());
			
			String metaInfo = "";
			for (int i =0;i<columnNames.length;i++){
				metaInfo += String.format("%s,%s,%s,%s/", columnNames[i], dataTypes[i], isKey[i], references[i]);
			}	
			
			block.setData(metaInfo.getBytes());
			storagemanager.writeBlock(0, NewFile,block);
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}

	@Override
	public void dropTable(String tableName) throws AbstractRecordManagerException {
		StorageManager sm = new StorageManager();
		try {
			sm.deleteFile(tableName);
		} catch (IOException e) {
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
		StorageManager sm = new StorageManager();
		
		ArrayList<Record> recs = new ArrayList<Record>();
		
		byte[] valueInBytes = MetaData.getType(dataType, value);
		int offset = mt.getColOffset(columnName);
		
		for(int i = 1; i < mt.dbFile.getTotalNumberOfBlocks(); i++)
		{
			if(!mt.isBlockFree(i-1))
			{
				try {
					Block b = (Block) sm.readBlock(i, mt.dbFile);
					byte[] block = b.getData();
					
					for(int slot = 0; slot < mt.slotsPerBlock ; slot++)
					{
						RecordID ri = new RecordID();
						ri.setBlockNumber(i-1);
						ri.setSlotNumber(slot);
						if(!mt.isFree(ri)){
							boolean match = true;
							int index = (slot*mt.slotSize)+offset;
							//match = Arrays.equals( Arrays.copyOfRange(block, index, index+valueInBytes.length) ,valueInBytes);
							for(int j = 0; j < valueInBytes.length; j++)
								if(block[index + j] != valueInBytes[j])
								{
									match = false;
									break;
								}
							if(match)
							{
								String[] values = mt.valuesToString(Arrays.copyOfRange(block, (slot*mt.slotSize), (slot*mt.slotSize)+mt.slotSize));
								Record r = new Record(mt.columnNames,mt.dataTypes, values,mt.references,ri);
								recs.add(r);
							}
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}	
		AbstractRecord[] result = new AbstractRecord[recs.size()];
		for(int i = 0; i < recs.size(); i++)
			result[i] = recs.get(i);	
		
		return result;
	}

	@Override
	public void deleteRecord(AbstractRecord r, String tableName) throws AbstractRecordManagerException {
		MetaData mt = openTable(tableName);	
	    RecordID index = ((Record)r).getKey();			
		mt.clearSlot(index);
		mt.saveTableMetaData();
	}

		
	@Override
	public void updateRecord(AbstractRecord r, String tableName) throws AbstractRecordManagerException {
		 MetaData mt = openTable(tableName);	
	       RecordID index = ((Record)r).getKey() ;
	       StorageManager sm = new StorageManager();
	       try {
				Block b = (Block) sm.readBlock( index.getBlockNumber() +1, mt.dbFile);
				byte[] array = b.getData();
				int slotlocation = mt.slotSize * index.getSlotNumber();
				
				String[] types = r.getDataTypes();
				String[] values = r.getValues();
				for(int i = 0, bi = slotlocation; i < types.length; i++)
				{
					byte[] tmp = MetaData.getType(types[i], values[i]);
					for(int j = 0; j < tmp.length; j++, bi++)
						array[bi] = tmp[j];
				}
				
				b.setData(array);
				sm.writeBlock(index.getBlockNumber() +1, mt.dbFile, b);
				
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
			Block b = (Block) sm.readBlock( index.getBlockNumber() +1, mt.dbFile);
			byte[] array = b.getData();
			int slotlocation = mt.slotSize * index.getSlotNumber();
			
			String[] types = r.getDataTypes();
			String[] values = r.getValues();
			int bi = slotlocation;
			for(int i = 0; i < types.length; i++)
			{
				byte[] tmp = MetaData.getType(types[i], values[i]);
				for(int j = 0; j < tmp.length; j++, bi++)
					array[bi] = tmp[j];
			}

			b.setData(array);
			sm.writeBlock(index.getBlockNumber()+1, mt.dbFile, b);
			mt.fillSlot(index);
			mt.saveTableMetaData();
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
		bitArray = new ArrayList<BitSet>();
	}
	
	public static byte[] toByteArray(BitSet bits, int size) {
		byte[] bytes = new byte[(size+7)/8];
	    for(int i = 0; i < size; i++)
	    {
	    	if(bits.get(i))
	    		bytes[i/8] |= 1<<(i%8);
	    }
	    return bytes;
	}
	
	public static BitSet toBitArray(byte[] bytes) {
		BitSet set = new BitSet(bytes.length*8);
		for (int i = 0; i < bytes.length; i++) {
			byte b = bytes[i];
	        int n = 7;
	        while(n >= 0)
	        {
	        	boolean isSet = (b & 0x80) != 0;
	        	set.set(i*8 + n, isSet);
	        	b <<=1;
	            n--;
	        }
	    }	
		return set;
	}
	
	
	public static byte[] getType(String type, String value)
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
	
	public static int getTypeSize(String type){
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
	
	
	
	public int getColOffset(String colName)
	{
		int result = 0;
		for(int i = 0; i < columnNames.length; i++)
		{
			if(columnNames[i].equalsIgnoreCase(colName))
				break;
			else{
				result += getTypeSize(dataTypes[i]);
			}
		}
		return result;
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
		BitSet b = new BitSet(((slotsPerBlock+7)/8)*8);
		b.set(0, b.size() - (b.size()-slotsPerBlock));
		bitArray.add(b);
	}
	
	public boolean isBlockFree(int b)
	{
		return bitArray.get(b).cardinality() == bitArray.get(b).size();
	}
	
	public void fillSlot(RecordID ri)
	{
		bitArray.get(ri.getBlockNumber()).clear(ri.getSlotNumber());
	}
	
	public void clearSlot(RecordID ri)
	{
		bitArray.get(ri.getBlockNumber()).set(ri.getSlotNumber());
	}
	
	public boolean isFree(RecordID ri)
	{
		return bitArray.get(ri.getBlockNumber()).get(ri.getSlotNumber());
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
				ri.setBlockNumber(i);
				ri.setSlotNumber(slot);
				break;
			}
		}
		if(!found){
			StorageManager sm = new StorageManager();
			try {
				sm.appendEmptyBlock(dbFile);
				addEmptyBlock();
				ri.setBlockNumber(bitArray.size()-1);
				ri.setSlotNumber(0);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return ri;
	}
	
	public String[] valuesToString(byte[] slot)
	{
		String[] values = new String[columnNames.length];
		
		for(int i = 0; i < columnNames.length; i++)
		{
			byte[] cell = new byte[MetaData.getTypeSize(dataTypes[i])];
			
			for(int b = 0; b < MetaData.getTypeSize(dataTypes[i]); b++)
				cell[b] = slot[ b + getColOffset(columnNames[i]) ];
			
			ByteBuffer wrapped = ByteBuffer.wrap(cell);
			values[i] = "";
			
			switch(dataTypes[i]){
			case "java.lang.Integer":
				values[i] += wrapped.getInt();
				break;
			case "java.lang.Boolean":
				values[i] += (cell[0] == 1? "True": "False");	
				break;
			case "java.util.Date":
				values[i] += wrapped.getLong();
				break;
			case "java.lang.String":
				values[i] += new String(cell);
				break;
			default:
				values[i] += new String(cell);
				break;
			}
		}
		return values;
	}
	
	public void saveTableMetaData()
	{
		StorageManager sm = new StorageManager();
		try {
			Block metaBlock = new Block();
			
			String metaInfo = "";
			
			for(int i = 0 ; i < columnNames.length ; i++)
				metaInfo += String.format("%s,%s,%s,%s/", columnNames[i], dataTypes[i], isKey[i], references[i]);
			
			byte[] header = metaInfo.getBytes();
			byte[] buffer = new byte[header.length + dbFile.getTotalNumberOfBlocks() * ((slotsPerBlock+7)/8)];//((mt.slotsPerBlock+31)/32)*4];
			
			for(int i = 0; i < header.length; i++)
					buffer[i] = header[i];
			
			for(int j = 0, index = header.length; j < bitArray.size(); j++)
			{
				byte[] tmp = MetaData.toByteArray(bitArray.get(j), bitArray.get(j).length());
				for(int i = 0; i < tmp.length; i++)
					buffer[index++] = tmp[i];		
			}
			
			metaBlock.setData(buffer);
			sm.writeBlock(0, dbFile, metaBlock);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

package minidb.recordmanager;

public interface IRecordManager {
	
	
	public void createTable(String tableName, String[] columnNames, String[] dataTypes, boolean[] isKey, String[] references) throws AbstractRecordManagerException;
	
	public void dropTable(String tableName) throws AbstractRecordManagerException;
	
	
	
	
	
	public AbstractRecord[] getRecord(String tableName, String columnName, String dataType, String value);
	
	public void deleteRecord(AbstractRecord r, String tableName) throws AbstractRecordManagerException;
	
	public void updateRecord(AbstractRecord r, String tableName) throws AbstractRecordManagerException;
	
	public void insertRecord(AbstractRecord r, String tableName) throws AbstractRecordManagerException;
	
	
	
	
	//These methods are due in Deliverable 4 !
	public void createIndex(String tableName, String columnName) throws AbstractRecordManagerException;
	public AbstractRecord getRecord(String tableName, RecordID rid) throws AbstractRecordManagerException;
	
}

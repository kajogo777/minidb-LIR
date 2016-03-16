package minidb.recordmanager;

public class RecordManager implements IRecordManager{

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

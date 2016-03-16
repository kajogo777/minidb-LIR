package minidb.recordmanager;

public abstract class AbstractRecord {
	
	private String[] columnNames;
	private String[] dataTypes;
	private String[] values;
	private String[] references;
	
	private RecordID key;
	
	public AbstractRecord(String[] columnNames, String[] dataTypes, String[] values, String[] references, RecordID key){
		
		this.columnNames = columnNames;
		this.dataTypes = dataTypes;
		this.values = values;
		this.references = references;
		this.key = key;
		
	}
	
	public abstract String[] getColumnNames();
	
	public abstract String[] getDataTypes();
	
	public abstract String[] getValues();
	
	public abstract String[] getReferences();
	
	public abstract RecordID getKey();
	
	public abstract void setKey(RecordID key);
	


}

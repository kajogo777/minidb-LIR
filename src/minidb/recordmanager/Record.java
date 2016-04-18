package minidb.recordmanager;

public class Record extends AbstractRecord{

	public Record(String[] columnNames, String[] dataTypes, String[] values, String[] references, RecordID key) {
		super(columnNames, dataTypes, values, references, key);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String[] getColumnNames() {
		return columnNames;
	}

	@Override
	public String[] getDataTypes() {
		return dataTypes;
	}

	@Override
	public String[] getValues() {
		return values;
	}

	@Override
	public String[] getReferences() {
		return references;
	}

	@Override
	public RecordID getKey() {
		return key;
	}

	@Override
	public void setKey(RecordID key) {
		// TODO Auto-generated method stub
		
	}

}

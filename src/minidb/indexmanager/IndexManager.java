package minidb.indexmanager;

import minidb.recordmanager.RecordID;

public class IndexManager implements IIndexManager{

	@Override
	public void init() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public AbstractBTree createBTree(String filePath, int n) throws AbstractIndexManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractBTree openBTree(String filePath) throws AbstractIndexManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void closeBTree(AbstractBTree t) throws AbstractIndexManagerException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteBTree(AbstractBTree t) throws AbstractIndexManagerException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getNumOfNodes(AbstractBTree t) throws AbstractIndexManagerException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumOfEntries(AbstractBTree t) throws AbstractIndexManagerException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public RecordID findKey(AbstractBTree t, Integer key) throws AbstractIndexManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertKey(AbstractBTree t, Integer key, RecordID r) throws AbstractIndexManagerException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteKey(AbstractBTree t, Integer key) throws AbstractIndexManagerException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void printBTree() throws AbstractIndexManagerException {
		// TODO Auto-generated method stub
		
	}

}

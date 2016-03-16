package minidb.indexmanager;
import minidb.recordmanager.RecordID;

public interface IIndexManager {
	
	public void init();
	
	
	//Create, open, delete, and close BTree file - must communicate with the storage manager
	public AbstractBTree createBTree(String filePath, int n) throws AbstractIndexManagerException;
	public AbstractBTree openBTree(String filePath)throws AbstractIndexManagerException;
	public void closeBTree(AbstractBTree t)throws AbstractIndexManagerException;
	public void deleteBTree(AbstractBTree t)throws AbstractIndexManagerException;
	
	//Access information about a BTree
	public int getNumOfNodes(AbstractBTree t)throws AbstractIndexManagerException;
	public int getNumOfEntries(AbstractBTree t)throws AbstractIndexManagerException;
	
	
	//Index access
	public RecordID findKey(AbstractBTree t, Integer key)throws AbstractIndexManagerException;
	public void insertKey(AbstractBTree t, Integer key, RecordID r)throws AbstractIndexManagerException;
	public void deleteKey(AbstractBTree t, Integer key)throws AbstractIndexManagerException;
		
	
	//for debug - be creative !
	public void printBTree()throws AbstractIndexManagerException;
	

}

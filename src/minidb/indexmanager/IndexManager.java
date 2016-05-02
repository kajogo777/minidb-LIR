package minidb.indexmanager;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import minidb.recordmanager.RecordID;
import minidb.storagemanager.Block;
import minidb.storagemanager.DBFile;
import minidb.storagemanager.StorageManager;

public class IndexManager implements IIndexManager{

	@Override
	public void init() {
		
	}

	@Override
	public AbstractBTree createBTree(String filePath, int n) throws AbstractIndexManagerException {
		StorageManager sm = new StorageManager();
		try {
			sm.createFile(filePath);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new BTree(n);
	}

	@Override
	public AbstractBTree openBTree(String filePath) throws AbstractIndexManagerException {
		BufferedReader reader;
		int n = 0;
		try {
			reader = Files.newBufferedReader(Paths.get(System.getProperty("user.dir") + "/conf/minidb.config"));
			reader.readLine();
			n = Integer.parseInt(reader.readLine().split("=")[1]);
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		StorageManager sm = new StorageManager();
		try {
			DBFile indexFile = (DBFile) sm.openFile(filePath);
			BTree bt = new BTree(n);
			
			Node root = getNode(indexFile, 0, n);
			connectLeaves(root);
			
			bt.setRoot(root);
	
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private Node getNode(DBFile f, int blckN, int degree) throws IOException
	{
		StorageManager sm = new StorageManager();
		Block curBlck = (Block) sm.readBlock(blckN, f);
		int offset = 0;
		Node result = null;

		if(curBlck.getData()[offset++] == 0)//if node is inner node
		{
			InnerNode n = new InnerNode(degree);
			n.setKeysN(ByteBuffer.wrap(Arrays.copyOfRange(curBlck.getData(), offset, offset+4)).getInt());
			offset += 4;
			
			//populate keys
			for(int i = 0; offset < n.getKeysN()*4; offset+= 4, i++)
				n.getKeys()[i] = ByteBuffer.wrap(Arrays.copyOfRange(curBlck.getData(), offset, offset+4)).getInt();
			
			//populate children
			for(int i = 0; offset < (n.getKeysN()+1)*4; offset+= 4, i++)
			{
				int childBlck = ByteBuffer.wrap(Arrays.copyOfRange(curBlck.getData(), offset, offset+4)).getInt();
				n.getChildren()[i] = getNode(f, childBlck, degree);
			}
			
			result = n;
		}else{//if node is leaf node
			LeafNode n = new LeafNode(degree);
			n.setKeysN(ByteBuffer.wrap(Arrays.copyOfRange(curBlck.getData(), offset, offset+4)).getInt());
			offset += 4;
			
			//populate keys
			for(int i = 0; offset < n.getKeysN()*4; offset+= 4, i++)
				n.getKeys()[i] = ByteBuffer.wrap(Arrays.copyOfRange(curBlck.getData(), offset, offset+4)).getInt();
			
			//populate pointers
			for(int i = 0; offset < n.getKeysN()*8; offset+= 8, i++)
			{
				RecordID rid = new RecordID();
				rid.setBlockNumber( ByteBuffer.wrap(Arrays.copyOfRange(curBlck.getData(), offset, offset+4)).getInt() );
				rid.setSlotNumber( ByteBuffer.wrap(Arrays.copyOfRange(curBlck.getData(), offset+4, offset+8)).getInt() );
				n.getValues()[i] = rid;
			}
			result = n;
		}
		return result;
	}

	private void connectLeaves(Node n)//BFS to connect leaves
	{
		Queue<Node> queue = new LinkedList<Node>();
		LeafNode prev = null;	
		queue.add(n);
		while(!queue.isEmpty()) {
			Node node = (Node)queue.remove();
			
			if(node instanceof InnerNode)
			{
				for(Node nc : ((InnerNode) node).getChildren())
					queue.add(nc);
			}else{
				if(prev == null)
					prev = (LeafNode) node;
				else{
					prev.setNext((LeafNode) node);
					prev = (LeafNode) node;
				}
			}
		}
	}
	
	@Override
	public void closeBTree(AbstractBTree t) throws AbstractIndexManagerException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteBTree(AbstractBTree t) throws AbstractIndexManagerException {
		StorageManager sm = new StorageManager();
		try {
			sm.deleteFile(((BTree)t).getFileName());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int getNumOfNodes(AbstractBTree t) throws AbstractIndexManagerException {
		return countHelper(((BTree)t).getRoot());
	}
	
	private int countHelper(Node n)
	{
		if(n instanceof LeafNode)
			return 1;
		
		int sum = 0;
		for(Node nc : ((InnerNode)n).getChildren())
			sum += countHelper(nc);
		
		return sum + 1;
	}

	@Override
	public int getNumOfEntries(AbstractBTree t) throws AbstractIndexManagerException {
		return countEntriesHelper(((BTree)t).getRoot());
	}
	
	private int countEntriesHelper(Node n)
	{
		if(n instanceof LeafNode)
			return ((LeafNode)n).getKeysN();
		
		int sum = 0;
		for(Node nc : ((InnerNode)n).getChildren())
			sum += countHelper(nc);
		
		return sum;
	}

	@Override
	public RecordID findKey(AbstractBTree t, Integer key) throws AbstractIndexManagerException {
		return searchHelper(key, ((BTree)t).getRoot());
	}
	
	private RecordID searchHelper(Integer key, Node n)
	{
		if(n instanceof LeafNode)
		{
			for(int i = 0; i < n.getKeysN(); i++)
				if(n.getKeys()[i].equals(key))
					return ((LeafNode) n).getValues()[i];
		}else{
			for(int i = 0; i < n.getKeysN(); i++)
				if(n.getKeys()[i].compareTo(key) >= 0)
					return searchHelper(key, ((InnerNode) n).getChildren()[i+1]);
		}
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

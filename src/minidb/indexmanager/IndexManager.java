package minidb.indexmanager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

import minidb.recordmanager.RecordID;
import minidb.storagemanager.Block;
import minidb.storagemanager.DBFile;
import minidb.storagemanager.StorageManager;

public class IndexManager implements IIndexManager{
	
	public static void main(String[] args) throws AbstractIndexManagerException
	{
		//test createBTree
		IndexManager im = new IndexManager();
		BTree bt = (BTree) im.createBTree("trialIndex", 3);
		
		InnerNode nroot  = new InnerNode(3);
		nroot.getKeys()[0] = 3;
		nroot.setKeysN(nroot.getKeysN()+1);
		
		InnerNode nl  = new InnerNode(3);
		nl.getKeys()[0] = 2;
		nl.setKeysN(nl.getKeysN()+1);
		
		InnerNode nr  = new InnerNode(3);
		nr.getKeys()[0] = 4;
		nr.getKeys()[1] = 5;
		nr.setKeysN(nr.getKeysN()+2);
		
		LeafNode l1 = new LeafNode(3);
		l1.getKeys()[0] = 1;
		l1.getValues()[0] = new RecordID();
		l1.getValues()[0].setBlockNumber(1);
		l1.getValues()[0].setSlotNumber(1);
		l1.setKeysN(l1.getKeysN()+1);
		
		LeafNode l2 = new LeafNode(3);
		l2.getKeys()[0] = 2;
		l2.getValues()[0] = new RecordID();
		l2.getValues()[0].setBlockNumber(1);
		l2.getValues()[0].setSlotNumber(2);
		l2.setKeysN(l2.getKeysN()+1);
		
		LeafNode l3 = new LeafNode(3);
		l3.getKeys()[0] = 3;
		l3.getValues()[0] = new RecordID();
		l3.getValues()[0].setBlockNumber(1);
		l3.getValues()[0].setSlotNumber(3);
		l3.setKeysN(l3.getKeysN()+1);
		
		LeafNode l4 = new LeafNode(3);
		l4.getKeys()[0] = 4;
		l4.getValues()[0] = new RecordID();
		l4.getValues()[0].setBlockNumber(1);
		l4.getValues()[0].setSlotNumber(4);
		l4.setKeysN(l4.getKeysN()+1);
		
		LeafNode l5 = new LeafNode(3);
		l5.getKeys()[0] = 5;
		l5.getValues()[0] = new RecordID();
		l5.getValues()[0].setBlockNumber(1);
		l5.getValues()[0].setSlotNumber(5);
		l5.getKeys()[1] = 6;
		l5.getValues()[1] = new RecordID();
		l5.getValues()[1].setBlockNumber(1);
		l5.getValues()[1].setSlotNumber(6);
		l5.getKeys()[2] = 7;
		l5.getValues()[2] = new RecordID();
		l5.getValues()[2].setBlockNumber(1);
		l5.getValues()[2].setSlotNumber(7);
		l5.setKeysN(l5.getKeysN()+3);
		
		l1.setNext(l2);
		l2.setNext(l3);
		l3.setNext(l4);
		l4.setNext(l5);
		l5.setNext(null);
		
		nl.getChildren()[0] = l1;
		nl.getChildren()[1] = l2;
		
		nr.getChildren()[0] = l3;
		nr.getChildren()[1] = l4;
		nr.getChildren()[2] = l5;
		
		nroot.getChildren()[0] = nl;
		nroot.getChildren()[1] = nr;
		
		bt.setRoot(nroot);
		//im.printBTree("trialIndex", output);
		
		//test closeBTree
		im.closeBTree(bt);
		
		//test openBTree
		bt = (BTree) im.openBTree("trialIndex");
		
		Queue<Node> queue = new LinkedList<Node>();	
		queue.add(bt.getRoot());
		while(!queue.isEmpty()) {
			Node node = (Node)queue.remove();	
			
			if(node instanceof InnerNode)
			{		
				System.out.printf("\n\nInnerNode: %d\nkeys: ", node.hashCode()%123);
				
				
				for(int i = 0; i < node.getKeysN(); i++)
				{
					System.out.printf("%3d  ",node.getKeys()[i]);
				}
				
				System.out.print("\nchildren: ");
				
				for(int i = 0; i < node.getKeysN()+1; i++)
				{
					System.out.printf("%3d  ",((InnerNode) node).getChildren()[i].hashCode()%123);
					queue.add(((InnerNode) node).getChildren()[i]);	
				}
			}else{
				System.out.printf("\n\nLeafNode: %d\nkeys: ", node.hashCode()%123);
				
				
				for(int i = 0; i < node.getKeysN(); i++)
				{
					System.out.printf("%3d  ",node.getKeys()[i]);
				}
				
				System.out.print("\nvalues: ");
				
				for(int i = 0; i < node.getKeysN(); i++)
				{
					System.out.printf("%d:%d  ",((LeafNode) node).getValues()[i].getBlockNumber(), ((LeafNode) node).getValues()[i].getSlotNumber());
				}
				
				System.out.printf("\nnext: %d", ((LeafNode) node).getNext() == null? 0 : ((LeafNode) node).getNext().hashCode()%123);
			}
		}
		System.out.println();
		
		//test findkey
		RecordID rid = im.findKey(bt, 6);
		System.out.printf("key %d in slot %d:%d\n", 6, rid.getBlockNumber(), rid.getSlotNumber());
		
		//test get number of nodes
		System.out.printf("number of nodes is %d\n", im.getNumOfNodes(bt));
		
		//test get number of entries
		System.out.printf("number of entries is %d\n", im.getNumOfEntries(bt));
	}

	@Override
	public void init() {
		
	}

	@Override
	public AbstractBTree createBTree(String filePath, int n) throws AbstractIndexManagerException {
		StorageManager sm = new StorageManager();
		try {
			sm.createFile(filePath);
			BTree b = new BTree(n);
			b.setFileName(filePath);
			return b;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
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
		//n = 3;//TODO for testing only
		StorageManager sm = new StorageManager();
		try {
			DBFile indexFile = (DBFile) sm.openFile(filePath);
			BTree bt = new BTree(n);
			
			Node root = getNode(indexFile, 0, n);
			connectLeaves(root);
			
			bt.setRoot(root);
			return bt;
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
			for(int i = 0; i < n.getKeysN(); offset+= 4, i++)
			{
				n.getKeys()[i] = ByteBuffer.wrap(Arrays.copyOfRange(curBlck.getData(), offset, offset+4)).getInt();
			}
			
			//populate children
			for(int i = 0; i < n.getKeysN()+1; offset+= 4, i++)
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
			for(int i = 0; i < n.getKeysN(); offset+= 4, i++)
				n.getKeys()[i] = ByteBuffer.wrap(Arrays.copyOfRange(curBlck.getData(), offset, offset+4)).getInt();
			
			//populate pointers
			for(int i = 0; i < n.getKeysN(); offset+= 8, i++)
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
				for(int i = 0; i < node.getKeysN()+1; i++)
					queue.add(((InnerNode) node).getChildren()[i]);
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
		StorageManager sm = new StorageManager();
		try {
			
			DBFile indexFile = (DBFile) sm.openFile(((BTree)t).getFileName());
			int blckN = 1;
			int curBlck = 0;
		
			Queue<Node> queue = new LinkedList<Node>();	
			queue.add(((BTree)t).getRoot());
			while(!queue.isEmpty()) {
				Node node = (Node)queue.remove();
				int offset = 1;
				ByteBuffer bf = ByteBuffer.allocate(4);
				Block blk = new Block();
				
				bf.putInt(node.getKeysN());
				for(byte b : bf.array())
					blk.getData()[offset++] = b;
				bf.clear();
				
				for(int i = 0; i < node.getKeysN(); i++)
				{
					bf.putInt(node.getKeys()[i]);
					for(byte b : bf.array())
						blk.getData()[offset++] = b;
					bf.clear();
				}

				if(node instanceof InnerNode)
				{	
					blk.getData()[0] = 0;
					
					for(int i = 0; i < node.getKeysN()+1; i++)
					{
						queue.add(((InnerNode) node).getChildren()[i]);
						bf.putInt(blckN++);
						for(byte b : bf.array())
							blk.getData()[offset++] = b;
						bf.clear();
					}
					
				}else{
					
					blk.getData()[0] = 1;
					
					for(int i = 0; i < node.getKeysN(); i++)
					{
						bf.putInt(((LeafNode)node).getValues()[i].getBlockNumber());
						for(byte b : bf.array())
							blk.getData()[offset++] = b;
						bf.clear();
						bf.putInt(((LeafNode)node).getValues()[i].getSlotNumber());
						for(byte b : bf.array())
							blk.getData()[offset++] = b;
						bf.clear();
					}
				}	
				sm.writeBlock(curBlck++, indexFile, blk);
			}
		
		} catch (IOException e) {
			e.printStackTrace();
		}
		
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
		
		for(int i = 0; i < n.getKeysN()+1; i++)
			sum += countHelper(((InnerNode)n).getChildren()[i]);
		
		return sum + 1;
	}

	@Override
	public int getNumOfEntries(AbstractBTree t) throws AbstractIndexManagerException {
		return countEntriesHelper(((BTree)t).getRoot());
	}
	
	private int countEntriesHelper(Node n)
	{
		if(n instanceof LeafNode)
		{
			int entries = ((LeafNode)n).getKeysN();
			if(((LeafNode)n).getNext() != null)
				entries += countEntriesHelper(((LeafNode)n).getNext());
			return entries;
		}
		return countEntriesHelper(((InnerNode)n).getChildren()[0]);
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
				if(key.compareTo(n.getKeys()[i]) == 0)
					return ((LeafNode) n).getValues()[i];
		}else{
			for(int i = 0; i < n.getKeysN(); i++)
				if(key.compareTo(n.getKeys()[i]) < 0)
					return searchHelper(key, ((InnerNode) n).getChildren()[i]);
			
			return searchHelper(key, ((InnerNode) n).getChildren()[n.getKeysN()]);
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
	public void printBTree(BTree t,BufferedWriter output) throws AbstractIndexManagerException, IOException {

		// TODO Auto-generated method stub

		Vector<Node> nodeList = new Vector();

		       

		        // put the root of the tree onto the stack to start the process

		 

		        nodeList.add(t.getRoot());



		        boolean done = false;

		        while(! done) {

		        // this vector will hold all the children of the nodes in the current level

		            Vector<Node> nextLevelList = new Vector();

		            String toprint = "";

		           

		            // for each node in the list convert it to a string and add any children to the nextlevel stack

		            for(int i=0; i < nodeList.size(); i++) {

		           

		            // get the node at position i

		                Node node = (Node)nodeList.elementAt(i);

		               

		                // convert the node into a string

		                toprint += node.toString() + " ";

		               

		                // if this is a leaf node we need only print the contents

		                if(node instanceof LeafNode) {

		                    done = true;

		                }

		                // if this is a tree node print the contents and populate

		                // the temp vector with nodes that node i points to

		                else

		                {

		                    for(int j=0; j < node.getKeysN()+1 ; j++) {

		                        //nextLevelList.add( ((InnerNode)node).getPointerAt(j) );

		                    nextLevelList.add( ((InnerNode)node).getChildren()[j] );

		                    }

		                }

		            }

		           

		            // print the level

		            output.write(toprint + System.getProperty("line.separator"));

		           

		            // go to the next level and print it

		            nodeList = nextLevelList;

		        }



		}

}

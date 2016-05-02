package minidb.indexmanager;

import minidb.recordmanager.RecordID;

public class BTree extends AbstractBTree{
	private Node root;
	private int degree;
	private String fileName;
	
	public BTree(int n)
	{
		degree = n;
		root = new LeafNode(degree);
	}
	
	public Node getRoot()
	{
		return root;
	}
	
	public void setRoot(Node n)
	{
		root = n;
	}
	
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}

abstract class Node{
	private int keysN;
	private Integer[] keys;

	public Node(int n)
	{
		keysN = 0;
		keys = new Integer[n];
	}
	
	public int getKeysN() 
	{
		return keysN;
	}

	public void setKeysN(int keysN) 
	{
		this.keysN = keysN;
	}
	
	public Integer[] getKeys() 
	{
		return keys;
	}

	public void setKeys(Integer[] keys) 
	{
		this.keys = keys;
	}
}

class InnerNode extends Node{
	private Node[] children;

	public InnerNode(int n)
	{
		super(n);
		children = new Node[n+1];
	}
	
	public Node[] getChildren() 
	{
		return children;
	}

	public void setChildren(Node[] children) 
	{
		this.children = children;
	}
}

class LeafNode extends Node{
	private RecordID[] values;
	private LeafNode next;

	public LeafNode(int n)
	{
		super(n);
		values = new RecordID[n];
	}
	
	public RecordID[] getValues() 
	{
		return values;
	}

	public void setValues(RecordID[] values) 
	{
		this.values = values;
	}
	
	public LeafNode getNext() 
	{
		return next;
	}

	public void setNext(LeafNode next) 
	{
		this.next = next;
	}
}
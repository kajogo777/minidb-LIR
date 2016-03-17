package minidb.storagemanager;

//smallest unit of storage that will be transfered to memory

public abstract class AbstractBlock {

	protected byte[] data;//changed from private to protected
	
	public AbstractBlock(){
		
		this.data = new byte[4096]; //default block size 4KB
		
	}
	
	public AbstractBlock(int blockSize){
		
		this.data = new byte[blockSize]; //should be called from the storage manager.
	}
	
	public abstract byte[] getData();
	
	public abstract void setData(byte[] d);
	
}

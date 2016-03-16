package minidb.storagemanager;

import java.io.IOException;

public interface IStorageManager {

	public void init();

	/* manipulating files on disk */
	public AbstractDBFile createFile(String fileName) throws IOException;

	public AbstractDBFile openFile(String fileName) throws IOException;

	public void closeFile(AbstractDBFile f) throws IOException;

	public void deleteFile(String fileName) throws IOException;

	
	
	/* reading blocks from disk */
	public AbstractBlock readBlock(int blockNum, AbstractDBFile f) throws IOException;

	public int getBlockPos(AbstractDBFile f) throws IOException;

	public AbstractBlock readFirstBlock(AbstractDBFile f) throws IOException;

	public AbstractBlock readPreviousBlock(AbstractDBFile f) throws IOException;

	public AbstractBlock readCurrentBlock(AbstractDBFile f) throws IOException;

	public AbstractBlock readNextBlock(AbstractDBFile f) throws IOException;

	public AbstractBlock readLastBlock(AbstractDBFile f) throws IOException;

	
	
	/* writing blocks to a file */
	public void writeBlock(int blockNum, AbstractDBFile f, AbstractBlock b) throws IOException;

	public void writeCurrentBlock(AbstractDBFile f, AbstractBlock b) throws IOException;

	public void appendEmptyBlock(AbstractDBFile f) throws IOException;

}

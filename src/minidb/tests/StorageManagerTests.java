package minidb.tests;

import java.io.IOException;
import junit.framework.TestCase;
import org.junit.Test;
import minidb.storagemanager.*;

public class StorageManagerTests extends TestCase {

	   @Test
	   public void testFileCreation1() throws IOException {
		   StorageManager manager = new StorageManager();
		   DBFile file = (DBFile)manager.createFile("File1");
		   assertEquals(file.getFileSize(),4096);
	   }

	   @Test
	   public void testFileCreation2() throws IOException {
		   StorageManager manager = new StorageManager();
		   DBFile file = (DBFile)manager.createFile("File1");
		   assertEquals(manager.getBlockPos(file),0);
	   }
	   
	   @Test
	   public void testFileOpen1() throws IOException {
		   StorageManager manager = new StorageManager();
		   DBFile file = (DBFile)manager.createFile("File1");
		   manager.closeFile(file);
		   
		   file = (DBFile)manager.openFile("File1");
		   assertEquals(file.getFileSize(),4096);
	   }	   
	   
	   @Test
	   public void testFileOpen2() throws IOException {
		   StorageManager manager = new StorageManager();
		   DBFile file = (DBFile)manager.createFile("File1");
		   manager.closeFile(file);
		   
		   file = (DBFile)manager.openFile("File1");
		   assertEquals(manager.getBlockPos(file),0);
	   }
	   
	   @Test
	   public void testReadBlock1() throws IOException {
		   StorageManager manager = new StorageManager();
		   DBFile file = (DBFile)manager.createFile("File1");
		   Block b = (Block) manager.readBlock(0, file);
		   byte [] result= new byte[4096];
		   assertTrue(areEqual(result,b.getData()));
	   }
	   
	   @Test
	   public void testWriteRead() throws IOException {
		   StorageManager manager = new StorageManager();
		   DBFile file = (DBFile)manager.createFile("File1");
		   
		   byte [] write = new byte[4096];
		   write[0] = 10;
		   write[5] = 20;
		   
		   Block block = new Block();
		   block.setData(write);
		   
		   manager.writeBlock(0, file, block);
		   
		   Block b = (Block) manager.readBlock(0, file);
		   assertTrue(areEqual(write,b.getData()));
	   }
	   
	   @Test
	   public void testWriteTwoBlocks() throws IOException {
		   StorageManager manager = new StorageManager();
		   DBFile file = (DBFile)manager.createFile("File1");
		   
		   byte [] write1 = new byte[4096];
		   write1[0] = 10;
		   write1[5] = 20;
		  
		   Block block1 = new Block();
		   block1.setData(write1);
		   
		   byte [] write2 = new byte[4096];
		   write2[2] = 15;
		   write2[20] = 30;

		   Block block2 = new Block();
		   block2.setData(write2);
		   
		   manager.writeBlock(0, file, block1);
		   manager.writeBlock(1, file, block2);		   
		   
		   Block b = (Block) manager.readBlock(1, file);
		   assertTrue(areEqual(write2,b.getData()));
	   }
	   
	   @Test
	   public void testCurrentBlockPos() throws IOException {
		   StorageManager manager = new StorageManager();
		   DBFile file = (DBFile)manager.createFile("File1");
		   
		   byte [] write1 = new byte[4096];
		   write1[0] = 10;
		   write1[5] = 20;
		  
		   Block block1 = new Block();
		   block1.setData(write1);
		   
		   byte [] write2 = new byte[4096];
		   write2[2] = 15;
		   write2[20] = 30;

		   Block block2 = new Block();
		   block2.setData(write2);
		   
		   manager.writeBlock(0, file, block1);
		   manager.writeBlock(1, file, block2);		   
		   
		   assertEquals(manager.getBlockPos(file),2);
	   }
	   
	   @Test
	   public void testFileSize() throws IOException {
		   StorageManager manager = new StorageManager();
		   DBFile file = (DBFile)manager.createFile("File1");
		   
		   byte [] write1 = new byte[4096];
		   write1[0] = 10;
		   write1[5] = 20;
		  
		   Block block1 = new Block();
		   block1.setData(write1);
		   
		   byte [] write2 = new byte[4096];
		   write2[2] = 15;
		   write2[20] = 30;

		   Block block2 = new Block();
		   block2.setData(write2);
		   
		   manager.writeBlock(0, file, block1);
		   manager.writeBlock(1, file, block2);		   
		   
		   assertEquals(file.getFileSize(),8192);
	   }
	   
	   @Test
	   public void testReadPreviousBlock() throws IOException {
		   StorageManager manager = new StorageManager();
		   DBFile file = (DBFile)manager.createFile("File1");
		   
		   byte [] write1 = new byte[4096];
		   write1[0] = 10;
		   write1[5] = 20;
		  
		   Block block1 = new Block();
		   block1.setData(write1);
		   
		   byte [] write2 = new byte[4096];
		   write2[2] = 15;
		   write2[20] = 30;

		   Block block2 = new Block();
		   block2.setData(write2);
		   
		   manager.writeBlock(0, file, block1);
		   manager.writeBlock(1, file, block2);		   
		   
		   byte [] read = manager.readPreviousBlock(file).getData();
		   
		   assertTrue(areEqual(read,write2));
	   }
	   
	   @Test
	   public void testReadFirstBlock() throws IOException {
		   StorageManager manager = new StorageManager();
		   DBFile file = (DBFile)manager.createFile("File1");
		   
		   byte [] write1 = new byte[4096];
		   write1[0] = 10;
		   write1[5] = 20;
		  
		   Block block1 = new Block();
		   block1.setData(write1);
		   
		   byte [] write2 = new byte[4096];
		   write2[2] = 15;
		   write2[20] = 30;

		   Block block2 = new Block();
		   block2.setData(write2);
		   
		   manager.writeBlock(0, file, block1);
		   manager.writeBlock(1, file, block2);		   
		   
		   byte [] read = manager.readFirstBlock(file).getData();
		   
		   assertTrue(areEqual(read,write1));
	   }
	   
	   @Test
	   public void testTwoFilesRead1() throws IOException {
		   StorageManager manager = new StorageManager();
		  
		   DBFile file1 = (DBFile)manager.createFile("File1");
		   DBFile file2 = (DBFile)manager.createFile("File2");
		   
		   byte [] write1 = new byte[4096];
		   write1[0] = 10;
		   write1[5] = 20;
		  
		   Block block1 = new Block();
		   block1.setData(write1);
		   
		   manager.writeCurrentBlock(file1, block1);
		   
		   byte [] write2 = new byte[4096];
		   write2[2] = 15;
		   write2[20] = 30;

		   Block block2 = new Block();
		   block2.setData(write2);

		   manager.writeCurrentBlock(file2, block2);
		   
		   byte [] read = manager.readFirstBlock(file1).getData();

		   assertTrue(areEqual(read,write1));
	   }
	   
	   @Test
	   public void testTwoFilesRead2() throws IOException {
		   StorageManager manager = new StorageManager();
		  
		   DBFile file1 = (DBFile)manager.createFile("File1");
		   DBFile file2 = (DBFile)manager.createFile("File2");
		   
		   byte [] write1 = new byte[4096];
		   write1[0] = 10;
		   write1[5] = 20;
		  
		   Block block1 = new Block();
		   block1.setData(write1);
		   
		   manager.writeCurrentBlock(file1, block1);
		   
		   byte [] write2 = new byte[4096];
		   write2[2] = 15;
		   write2[20] = 30;

		   Block block2 = new Block();
		   block2.setData(write2);

		   manager.writeCurrentBlock(file2, block2);
		   
		   byte [] read = manager.readFirstBlock(file2).getData();
		   		   		   
		   assertTrue(areEqual(read,write2));
	   }
	   
	   @Test
	   public void testTwoFilesOpen() throws IOException {
		   StorageManager manager = new StorageManager();
		  
		   DBFile file1 = (DBFile)manager.createFile("File1");
		   DBFile file2 = (DBFile)manager.createFile("File2");
		   
		   byte [] write1 = new byte[4096];
		   write1[0] = 10;
		   write1[5] = 20;
		  
		   Block block1 = new Block();
		   block1.setData(write1);
		   
		   manager.writeCurrentBlock(file1, block1);
		   
		   byte [] write2 = new byte[4096];
		   write2[2] = 15;
		   write2[20] = 30;

		   Block block2 = new Block();
		   block2.setData(write2);

		   manager.writeCurrentBlock(file2, block2);
		   
		   manager.closeFile(file1);
		   DBFile file = (DBFile) manager.openFile("File1");
		   
		   assertEquals(file.getFileSize(),4096);
	   }
	   
	   private boolean areEqual(byte [] array1, byte [] array2) {
		   if(array1.length != array2.length)
			   return false;
		   for(int i = 0 ; i < array1.length ; i++)
			   if(array1[i] != array2[i])
				   return false;
		   return true;
	   }
}

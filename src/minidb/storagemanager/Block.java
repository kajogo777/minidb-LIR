package minidb.storagemanager;

public class Block extends AbstractBlock{

	public Block()
	{
		super();
	}
	public Block(int blockSize) {
		super(blockSize);
	}
	
	@Override
	public byte[] getData() {
		return data;
	}

	@Override
	public void setData(byte[] d) {
		// TODO Auto-generated method stub
//		int freeByte = -1;
//		
//		for(int i = 0; i < data.length && freeByte < 0; i++)
//			if(data[i] == 0)
//				freeByte = i;
//		
//		for(int i = freeByte, j = 0; j < d.length; i++, j++)
//			data[i] = d[j];

		for(int i = 0; i < d.length; i++)
			data[i] = d[i];
	}

}

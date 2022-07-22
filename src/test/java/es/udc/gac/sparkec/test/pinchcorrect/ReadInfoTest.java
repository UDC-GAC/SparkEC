package es.udc.gac.sparkec.test.pinchcorrect;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

import es.udc.gac.sparkec.pinchcorrect.ReadInfo;

public class ReadInfoTest {
	
	@Test
	public void serializationTest() throws IOException, ClassNotFoundException {
		ReadInfo base = new ReadInfo(
			1234567890L,
			true,
			(short)3,
			'A',
			'4',
			(short)15
		);
		
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(bs);
		
		os.flush();
		byte[] bytes = bs.toByteArray();
		os.writeObject(base);
		os.flush();
		os.close();
		bytes = bs.toByteArray();
		
		ByteArrayInputStream bs2 = new ByteArrayInputStream(bytes);
		ObjectInputStream is = new ObjectInputStream(bs2);
		ReadInfo result = (ReadInfo)is.readObject();
		is.close();
		
		assertEquals(result, base);
		
		
	}
}

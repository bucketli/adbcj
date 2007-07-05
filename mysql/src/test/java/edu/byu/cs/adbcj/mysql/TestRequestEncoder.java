package edu.byu.cs.adbcj.mysql;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.junit.Assert;
import org.junit.Test;

public class TestRequestEncoder {

	@Test
	public void testHeaderEncoding() throws Exception {
		final int length = 0xabcdef;
		final int packetNumber = (byte)0xff;
		final AtomicBoolean invokedEncode = new AtomicBoolean(false);
		final AtomicBoolean invokedWrite = new AtomicBoolean(false);

		final Request myRequest = new Request() {
			@Override
			int getLength() {
				return length;
			}
			@Override
			public byte getPacketNumber() {
				return packetNumber;
			}
		};
		
		RequestEncoder<Request> encoder = new RequestEncoder<Request>() {
			@Override
			protected void encode(IoSession session, Request request, ByteBuffer buffer) {
				invokedEncode.set(true);

				Assert.assertSame(myRequest, request);
				Assert.assertEquals(length, request.getLength());
				Assert.assertEquals(length + REQUEST_HEADER_SIZE, buffer.capacity());
				
				Assert.assertNotNull(buffer);
			}
			public Set<Class<Request>> getMessageTypes() {
				return null;
			}
		};

		encoder.encode(null, myRequest, new ProtocolEncoderOutput() {
			public WriteFuture flush() {
				return null;
			}

			public void mergeAll() {
			}

			public void write(ByteBuffer buf) {
				invokedWrite.set(true);
				Assert.assertEquals((byte)0xef, buf.get());
				Assert.assertEquals((byte)0xcd, buf.get());
				Assert.assertEquals((byte)0xab, buf.get());
				
				Assert.assertEquals(packetNumber, buf.get());
			}
		});
		
		Assert.assertTrue("The RequestEncoder.encode(IoSession, Request, ByteBuffer) method was never called", invokedEncode.get());
		Assert.assertTrue("The ProtocolEncoderOutput.write(ByteBuffer) method was never called", invokedWrite.get());
	}

}

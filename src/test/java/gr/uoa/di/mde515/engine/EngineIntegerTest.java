package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.engine.buffer.IntegerSerializer;

import org.junit.Test;

import static org.junit.Assert.fail;

public class EngineIntegerTest {

	final Engine<Integer, Integer> eng;

	public EngineIntegerTest() {
		this.eng = Engine.newInstance(IntegerSerializer.INSTANCE,
			IntegerSerializer.INSTANCE);
	}
	@Test
	public void test() {
		fail("Not yet implemented");
	}
}

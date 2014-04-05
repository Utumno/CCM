package gr.uoa.di.mde515.engine;

import gr.uoa.di.mde515.engine.buffer.Page;

public class Lock {

	Type t;
	Page p;

	enum Type {
		IX, IS;
	}
}

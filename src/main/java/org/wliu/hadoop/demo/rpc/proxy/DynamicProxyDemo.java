package org.wliu.hadoop.demo.rpc.proxy;

import java.lang.reflect.Proxy;

public class DynamicProxyDemo {

	public static void main(String[] args) {
		Invoker invoker = new Invoker(new ClassA());
		AbstractClass a = (AbstractClass) Proxy.newProxyInstance(AbstractClass.class.getClassLoader(),
				new Class[] { AbstractClass.class }, invoker);
		a.show();
	}

}

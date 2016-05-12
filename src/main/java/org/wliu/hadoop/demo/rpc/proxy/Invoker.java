package org.wliu.hadoop.demo.rpc.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class Invoker implements InvocationHandler {

	private AbstractClass proxyObject;

	public Invoker(AbstractClass objClass) {
		this.proxyObject = objClass;
	}

	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

		System.out.println("proxy start.");
		if (args != null) {
			for (Object obj : args) {
				System.out.println("param:" + obj.toString());
			}
		}
		Object ret = method.invoke(this.proxyObject, args);
		System.out.println("proxy start.");
		return ret;
	}

}

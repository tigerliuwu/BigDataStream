package com.zx.bigdata.bean.datadef.validate.dict;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * 数据字典
 * <p>
 * 目前还没有实现，需要参考文件中的格式和内容，对数据字典的结构进行重构。
 * 
 * @author liuwu
 *
 */
public class DictCache {

	private static final Logger LOG = Logger.getLogger(DictCache.class);

	// Map<fieldName, Map<Value, Description>>
	private static final LinkedHashMap<String, LinkedHashMap<String, String>> reportDict = new LinkedHashMap<String, LinkedHashMap<String, String>>();

	/**
	 * 根据字段的标识符，获取该标识符所有可能的值
	 * 
	 * @param fieldName
	 * @return
	 */
	public static Collection<String> getFieldBy(String fieldName) {
		if (reportDict.containsKey(fieldName)) {
			return reportDict.get(fieldName).keySet();
		}
		LOG.warn("字段标识符：\"" + fieldName + "\"在数据字典中不存在。");
		return null;
	}

	/**
	 * 判断字段的值(key)在该字段对应的数据字典中是否存在
	 * 
	 * @param fieldName
	 * @param key
	 * @return boolean 存在返回true；否则返回false
	 */
	public static boolean containsKey(String fieldName, String key) {
		Collection<String> set = getFieldBy(fieldName);
		if (set == null || set.isEmpty()) {
			return false;
		} else {
			return set.contains(key);
		}
	}

	public static void setDict(LinkedHashMap<String, LinkedHashMap<String, String>> dict) {
		for (Map.Entry<String, LinkedHashMap<String, String>> entry : dict.entrySet()) {
			reportDict.put(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * 根据文件名构建字典
	 * 
	 * @param path
	 */
	public static void buildDict(String fileName) {
		// TODO 实现从从文件中读取文件中的内容，构建一个字典
		LOG.warn("目前使用的是测试字典，需要实现DictCache.buildDict方法。");
		// 登记状态
		LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
		map.put("1", "正常");
		map.put("2", "注销");
		map.put("3", "停业");
		map.put("4", "其他");
		reportDict.put("STATUS", map);

		// 注册类型
		map = new LinkedHashMap<String, String>();
		map.put("4000", "个体经营");
		map.put("4100", "个体户口");
		map.put("4200", "个人合伙");
		map.put("4300", "个人");
		map.put("4310", "大陆公民");
		map.put("4320", "港澳台胞");
		map.put("4330", "外国人");
		reportDict.put("REGITYPE", map);

	}

}

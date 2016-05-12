package com.zx.bigdata.bean.datadef.validate;

public enum BandkOrNonBankEnum {

	NON_BANK("E");

	String name;

	BandkOrNonBankEnum(String name) {
		this.name = name;
	}

	public static BandkOrNonBankEnum fromString(String name) {
		for (BandkOrNonBankEnum vl : BandkOrNonBankEnum.values()) {
			if (vl.name.equalsIgnoreCase(name)) {
				return vl;
			}
		}
		throw new RuntimeException("保送机构代码第一个首字母:" + name + " 不合法！");
	}

}

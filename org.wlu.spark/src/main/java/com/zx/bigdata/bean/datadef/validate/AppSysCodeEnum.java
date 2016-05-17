package com.zx.bigdata.bean.datadef.validate;

public enum AppSysCodeEnum {

	APP_NONBANK_PERSONAL("2"),

	APP_NONBANK_CORP("1");

	private String app_sys_code;

	AppSysCodeEnum(String code) {
		this.app_sys_code = code;
	}

	public static AppSysCodeEnum fromString(String reportType) {
		for (AppSysCodeEnum type : AppSysCodeEnum.values()) {
			if (reportType.equalsIgnoreCase(type.app_sys_code)) {
				return type;
			}
		}
		return null;
	}

	public String getAppSysCode() {
		return this.app_sys_code;
	}

}

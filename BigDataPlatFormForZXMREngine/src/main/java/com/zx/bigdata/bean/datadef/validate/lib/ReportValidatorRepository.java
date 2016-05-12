package com.zx.bigdata.bean.datadef.validate.lib;

import java.util.HashMap;
import java.util.Map;

import com.zx.bigdata.bean.datadef.validate.AppSysCodeEnum;
import com.zx.bigdata.bean.datadef.validate.inter.IZXValidator;

public class ReportValidatorRepository {

	private static Map<AppSysCodeEnum, IZXValidator> cache = new HashMap<AppSysCodeEnum, IZXValidator>();

	private ReportValidatorRepository() {

	}

	public static IZXValidator get(String appSysCode) {
		AppSysCodeEnum appSysCodeEnum = AppSysCodeEnum.fromString(appSysCode);

		IZXValidator validator = cache.get(appSysCodeEnum);
		if (validator != null) {
			return validator;
		}
		switch (appSysCodeEnum) {
		case APP_NONBANK_PERSONAL:
			validator = new EPersonalValidatorLib();
			break;
		default:
			throw new RuntimeException("未知应用系统代码");
		}

		cache.put(appSysCodeEnum, validator);

		return validator;

	}

}

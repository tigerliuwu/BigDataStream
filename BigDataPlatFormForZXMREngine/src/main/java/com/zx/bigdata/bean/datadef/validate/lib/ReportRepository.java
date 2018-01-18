package com.zx.bigdata.bean.datadef.validate.lib;

import java.util.HashMap;
import java.util.Map;

import com.zx.bigdata.bean.datadef.validate.AppSysCodeEnum;
import com.zx.bigdata.bean.datadef.validate.inter.IZXValidator;

public class ReportRepository {

	private static Map<AppSysCodeEnum, IZXValidator> valLib_cache = new HashMap<AppSysCodeEnum, IZXValidator>();

	private ReportRepository() {

	}

	public static IZXValidator getValidatorLib(String appSysCode) {
		AppSysCodeEnum appSysCodeEnum = AppSysCodeEnum.fromString(appSysCode);

		IZXValidator validator = valLib_cache.get(appSysCodeEnum);
		if (validator != null) {
			return validator;
		}
		switch (appSysCodeEnum) {
		case APP_NONBANK_PERSONAL:
			validator = new EPersonalValidatorLib();
			break;
		case APP_NONBANK_CORP:
			// TODO

		default:
			throw new RuntimeException("未知应用系统代码");
		}

		valLib_cache.put(appSysCodeEnum, validator);

		return validator;

	}

}

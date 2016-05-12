package com.zx.bigdata.bean.datadef.validate.inter;

import com.zx.bigdata.bean.datadef.rules.ZXValidatorPrototype;

/**
 * 
 * @author liuwu
 *
 */
public interface IZXValidator {

	public ZXValidatorPrototype getValidator(String reportType);

}

package com.zx.bigdata.bean.datadef.rules;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;

/**
 * 该类中所有的实现方法均代表一个校验规则，
 * 新增加的每个方法，需要在RuleMethodEnum进行注册以及在RulesExecutor对该方法进行调用
 *
 */
public class RuleMethod {
	
	/**校验该列是否为空
	 * @param object 任何字段值
	 * @return boolean 如果是null或者为空，则返回true，否则返回false
	 */
	public boolean isNullable(Object object) {
		
		if(object==null||object.toString().trim().equals("")){
			
			return true;	
		}else{
			
			return false;
			
		}
				
	}
	
	
	/**校验数据类型N,AN,ANC
	 * @param object 被校验的字段值
	 * @param dataType 约束类型，可以是"N","AN","ANC"
	 * @return boolean 通过校验返回true，不通过校验返回false
	 */
	public boolean checkDataType(Object object,String dataType) {

		if(object==null||object.toString().trim().equals("")){
			return true;	
		}else{
			
			char[] strTochar = object.toString().toCharArray();
			
			if(dataType.trim().equals("N")){
				for(int i = 0;i<strTochar.length;i++){
					
					if(!(getASC(strTochar[i])>=Integer.parseInt("30",16) && getASC(strTochar[i])<=Integer.parseInt("39",16))){	
						return false;
					}
				}
			}
			
			if(dataType.trim().equals("AN")){
				for(int i = 0;i<strTochar.length;i++){
					
					if(!(getASC(strTochar[i])>=Integer.parseInt("20",16) && getASC(strTochar[i])<=Integer.parseInt("7E",16))){
						return false;
					}
				}
			}
			
			if(dataType.trim().equals("ANC")){
				return true;
			}
			return true;	
		}
	}
	
	
	/**获取ASC码值
	 * @param chr 想要获取ASC码的字符
	 * @return int 返回字符的ASC码值
	 */
	private int getASC(char chr){
		
		int ascNO = chr;
		return ascNO;
		
	}
	
	
	
	/**校验数据项的长度
	 * @param object 被校验的字段值
	 * @param length 约束的数据长度
	 * @return boolean 通过校验返回true，不通过校验返回false
	 */
	public boolean checkLength(Object object,int length){

		if(object.toString().length()==length){
			return true;
		}else{
			return false;
		}

	}
	
	
	/**根据开始位置和结束位置，截取数据项
	 * @param object 需要被截取的数据项
	 * @param sPosition 截取的开始位置
	 * @param ePosition 截取的结束位置
	 * @return String 通过截取的字符串。如果出现异常，返回空字符串
	 */
	public String getStrByStartEnd(Object object,int sPosition,int ePosition){
		try{
			if(object!=null){
				return object.toString().substring(sPosition-1, ePosition);
			}else{
				return null;
			}
		}catch(Exception e){
			return "";
		}
	}
	
	
	/**校验数据项是否落于连续的值域之内
	 * @param object 需要被截取的数据项
	 * @param start 值域起始值
	 * @param end 值域终止值
	 * @return boolean 通过校验返回true，不通过校验返回false
	 */
	public boolean dataInRange(Object object,float start,float end){
		System.out.println("进入校验datainrange");
		if(object !=null && !object.toString().trim().equals("")){
			float checkData = Float.parseFloat(object.toString().trim());
			
			if(checkData>=start && checkData<=end){
				System.out.println("");
				return true;
			}else{
				System.out.println("");
				return false;
			}
		}else{
			System.out.println("");
			return false;
		}
		
	}
	
	
	/**校验数据项是否存在于离散值之中
	 * @param object 需要被截取的数据项
	 * @param dictionaryStr 离散的字典值字符串，各离散值之间用逗号分割
	 * @return boolean 通过校验返回true，不通过校验返回false
	 */
	public boolean dataInSet(Object object,String dictionaryStr){
		System.out.println("");
		HashSet<String> dictionarySet = new HashSet<String>(); 
		
		for(String tmp:dictionaryStr.split(",")){
			if (!tmp.trim().isEmpty()) {
				dictionarySet.add(tmp);
			}
		}
		
		if(dictionarySet.contains(object.toString().trim())){
			System.out.println("");
			return true;
		}else{
			System.out.println("");
			return false;
		}
		
	}
	
	
	/**校验数据项是否满足日期格式要求,日期均大于2006年，并且小于当前日期
	 * @param date 需要被校验的数据项
	 * @param formatType 可以是"yyyymm","yyyymmdd","yyyymmddhhmmss"
	 * @return boolean 通过校验返回true，不通过校验返回false
	 */
	public boolean checkDate(String date,String formatType){
		System.out.println("");
		
		Calendar nowDate = Calendar.getInstance();
		
		try {
		
			if(date.trim().length() == formatType.trim().length()){
			
				int yearStr = Integer.parseInt(date.trim().substring(0, 4));
					
				SimpleDateFormat dateFormat = new SimpleDateFormat(formatType);
				
				Date ddate = dateFormat.parse(date);
				
				Calendar textDate = Calendar.getInstance();
				
				textDate.setTime(ddate);
				
				if(yearStr>=2007 && (nowDate.after(textDate)||nowDate.equals(textDate))){
					System.out.println("");
					return true;
					
				}else{
					System.out.println("");
					return false;
					
				}
				
			}else{
				System.out.println("");
				return false;
				
			}
		
		} catch (ParseException e) {
			// TODO Auto-generated catch block	
			e.printStackTrace();
			return false;

		}
		
	}
	
	
	
	/**校验身份证号码是否有效
	 * @param idNo  身份证号码
	 * @return boolean 通过校验返回true，不通过校验返回false
	 */
	public boolean checkIDCardValidateCode(String idNo){
		
		String id17 = idNo.substring(0,17);
		
		//十七位数字本体码权重
		int[] weight={7,9,10,5,8,4,2,1,6,3,7,9,10,5,8,4,2};  
		//mod11,对应校验码字符值 
	    char[] validate={ '1','0','X','9','8','7','6','5','4','3','2'};       
	    	
	    int sum=0;
	    int mode=0;
	    for(int i=0;i<id17.length();i++){
	    	
	    	sum=sum+Integer.parseInt(String.valueOf(id17.charAt(i)))*weight[i];
	        
	    }
	        
	    	mode=sum%11;	
	        
	        if(idNo.substring(17, 18).equals(String.valueOf(validate[mode]))){
	        	
	        	return true;
	        	
	        }else{
	        	
	        	return false;
	        	
	        }
	}
	
	/**
	 * 数据格式版本号校验，格式为N.N
	 * @param version
	 * @return
	 */
	public boolean isVersionFormat(String version) {
		if (version == null || version.length() != 3 || version.charAt(1) != '.') {
			return false;
		}
		if (version.charAt(0) < '0' || version.charAt(0) > '9') {
			return false;
		}
		if (version.charAt(2) < '0' || version.charAt(2) > '9') {
			return false;
		}
		return true;
	}
	
	public static void main(String[] args) {
		RuleMethod method = new RuleMethod();
		boolean result = method.checkIDCardValidateCode("430426198802025045");
		if (result) {
			
		}
		System.out.println("hi,there "+result);
	}

}

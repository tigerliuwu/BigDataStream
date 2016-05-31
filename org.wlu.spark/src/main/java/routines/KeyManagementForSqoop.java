package routines;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class KeyManagementForSqoop {

	/**
	 * primaryKeyExists: Detect the presence of a primary key
	 * {talendTypes} boolean
	 * {Category} User Defined
	 * {example} primaryKeyExists("ddl")
	 */
	public static boolean primaryKeyExists(String ddl) {
		boolean primaryKeyFound = false;
		if (ddl.contains("PRIMARY KEY"))
			primaryKeyFound = true;
		return primaryKeyFound;
	}

	/**
	 * determineSplitColumn: Determines column for sqoop split
	 * {talendTypes} String
	 * {Category} User Defined
	 * {example} primaryKeyExists("ddl")
	 */
	public static String determineSplitColumn(String ddl) {
		String splitColumn = new String();
		if (ddl.contains("KEY")) {
			Pattern pattern = Pattern.compile("KEY.*?\\(`(.*?)`");
			Matcher matcher = pattern.matcher(ddl);

			if (matcher.find()) {
				String keys = matcher.group(1);
				splitColumn = keys.trim();
			}
		}
		return splitColumn;
	}
}

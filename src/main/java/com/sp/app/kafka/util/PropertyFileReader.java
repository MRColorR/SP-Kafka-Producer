package com.sp.app.kafka.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**Helper class to read properties from files
 * 
 * @author Rengo
 *
 */

public class PropertyFileReader {
	private static final Logger logger = Logger.getLogger(PropertyFileReader.class);
	private static Properties prop = new Properties();
	public static Properties readPropertyFile() throws Exception {
		if (prop.isEmpty()) {
			InputStream input = PropertyFileReader.class.getClassLoader().getResourceAsStream("sp-kafka.properties");
			try {
				prop.load(input);
			} catch (IOException e) {
				logger.error(e);
				throw e;
			} finally {
				if (input != null) {
					input.close();
				}
			}
		}
		return prop;
	}
}

package com.sp.app.kafka.util;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sp.app.kafka.vo.SPData;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/** Utility Class to convert SPData java object to a JSON String
 * 
 * @author Rengo
 *
 */

public class SPDataEncoder implements Encoder<SPData>{
	
	private static final Logger logger = Logger.getLogger(SPDataEncoder.class);
	
	private static ObjectMapper objectMapper = new ObjectMapper();		
	public SPDataEncoder(VerifiableProperties verifiableProperties) {

    }

	@Override
	public byte[] toBytes(SPData arg0) {
		try {
			String msg = objectMapper.writeValueAsString(arg0);
			logger.info(msg);
			return msg.getBytes();
		} catch (JsonProcessingException e) {
			logger.error("Error in Serialization", e);
		}
		return null;
	}
	
	
}

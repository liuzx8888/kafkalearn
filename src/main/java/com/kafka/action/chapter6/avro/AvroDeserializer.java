package com.kafka.action.chapter6.avro;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public T deserialize(String Topic, byte[] data) {
		// TODO Auto-generated method stub
		if(data == null) {
            return null;
        }
		AvroStockQuotation stock = new AvroStockQuotation();
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        DatumReader<T> userDatumReader = new SpecificDatumReader<>(stock.getSchema());
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
        try {
            stock = (AvroStockQuotation) userDatumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (T) stock;


	}

}

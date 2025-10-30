package org.flink.serializers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Custom Kryo serializer for Avro's GenericData.Array to avoid NullPointerException
 * during serialization/deserialization.
 */
public class AvroIntArraySerializer extends Serializer<Collection<?>> implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    public AvroIntArraySerializer() {
        // Accept null values
        this.setAcceptsNull(true);
    }
    
    @Override
    public void write(Kryo kryo, Output output, Collection<?> object) {
        if (object == null) {
            output.writeInt(-1);
            return;
        }
        
        // Write the size of the collection
        output.writeInt(object.size());
        
        // Write each element, handling nulls
        for (Object value : object) {
            if (value == null) {
                output.writeBoolean(false);
            } else {
                output.writeBoolean(true);
                if (value instanceof Integer) {
                    output.writeInt((Integer) value);
                } else {
                    // For non-integer values, write as string
                    output.writeString(value.toString());
                }
            }
        }
    }
    
    @Override
    public Collection<?> read(Kryo kryo, Input input, Class<Collection<?>> type) {
        int size = input.readInt();
        if (size == -1) {
            return null;
        }
        
        // Create a new ArrayList to avoid Avro's IntArray implementation
        ArrayList<Integer> result = new ArrayList<>(size);
        
        // Read each element, handling nulls
        for (int i = 0; i < size; i++) {
            boolean isNotNull = input.readBoolean();
            if (isNotNull) {
                result.add(input.readInt());
            } else {
                result.add(null);
            }
        }
        
        return result;
    }
} 

package org.flink;

import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.flink.serializers.PrimitiveArraySerializer;
import org.flink.serializers.AvroIntArraySerializer;

public class StreamEnvironment {
    private static StreamExecutionEnvironment env = null;

    private StreamEnvironment() {
    }

    public static StreamExecutionEnvironment getEnvInstance() {
        if (env == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();

            // Register primitive array serializers with correct type parameters
            env.getConfig().registerTypeWithKryoSerializer(int[].class, 
                new PrimitiveArraySerializer<>(int[].class));
            env.getConfig().registerTypeWithKryoSerializer(Integer[].class, 
                new PrimitiveArraySerializer<>(Integer[].class));
            env.getConfig().registerTypeWithKryoSerializer(long[].class, 
                new PrimitiveArraySerializer<>(long[].class));
            env.getConfig().registerTypeWithKryoSerializer(Long[].class, 
                new PrimitiveArraySerializer<>(Long[].class));
            env.getConfig().registerTypeWithKryoSerializer(double[].class, 
                new PrimitiveArraySerializer<>(double[].class));
            env.getConfig().registerTypeWithKryoSerializer(Double[].class, 
                new PrimitiveArraySerializer<>(Double[].class));
            env.getConfig().registerTypeWithKryoSerializer(float[].class, 
                new PrimitiveArraySerializer<>(float[].class));
            env.getConfig().registerTypeWithKryoSerializer(Float[].class, 
                new PrimitiveArraySerializer<>(Float[].class));
            env.getConfig().registerTypeWithKryoSerializer(boolean[].class, 
                new PrimitiveArraySerializer<>(boolean[].class));
            env.getConfig().registerTypeWithKryoSerializer(Boolean[].class, 
                new PrimitiveArraySerializer<>(Boolean[].class));
            env.getConfig().registerTypeWithKryoSerializer(byte[].class, 
                new PrimitiveArraySerializer<>(byte[].class));
            env.getConfig().registerTypeWithKryoSerializer(Byte[].class, 
                new PrimitiveArraySerializer<>(Byte[].class));
            env.getConfig().registerTypeWithKryoSerializer(short[].class, 
                new PrimitiveArraySerializer<>(short[].class));
            env.getConfig().registerTypeWithKryoSerializer(Short[].class, 
                new PrimitiveArraySerializer<>(Short[].class));
            env.getConfig().registerTypeWithKryoSerializer(char[].class, 
                new PrimitiveArraySerializer<>(char[].class));
            env.getConfig().registerTypeWithKryoSerializer(Character[].class, 
                new PrimitiveArraySerializer<>(Character[].class));
            
            // Register Avro-specific serializers
            env.getConfig().registerTypeWithKryoSerializer(GenericData.Array.class, 
                new AvroIntArraySerializer());
            
            // Register Avro's specific implementations of primitive arrays
            env.getConfig().registerTypeWithKryoSerializer(
                org.apache.avro.generic.PrimitivesArrays.IntArray.class, 
                new AvroIntArraySerializer());
                
            // Also register for Avro's UTF8 strings
            env.getConfig().registerKryoType(Utf8.class);
        }
        return env;
    }
}

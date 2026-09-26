package io.github.mannkostir.flink.serializers;

import java.io.Serializable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class PrimitiveArraySerializer<T> extends Serializer<T> implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private final Class<T> type;
    
    public PrimitiveArraySerializer(Class<T> type) {
        this.type = type;
        this.setAcceptsNull(true);
    }
    
    @Override
    public void write(Kryo kryo, Output output, T object) {
        if (object == null) {
            output.writeInt(-1);
            return;
        }
        
        if (type == int[].class) {
            int[] array = (int[]) object;
            output.writeInt(array.length);
            for (int value : array) {
                output.writeInt(value);
            }
        } else if (type == long[].class) {
            long[] array = (long[]) object;
            output.writeInt(array.length);
            for (long value : array) {
                output.writeLong(value);
            }
        } else if (type == double[].class) {
            double[] array = (double[]) object;
            output.writeInt(array.length);
            for (double value : array) {
                output.writeDouble(value);
            }
        } else if (type == float[].class) {
            float[] array = (float[]) object;
            output.writeInt(array.length);
            for (float value : array) {
                output.writeFloat(value);
            }
        } else if (type == boolean[].class) {
            boolean[] array = (boolean[]) object;
            output.writeInt(array.length);
            for (boolean value : array) {
                output.writeBoolean(value);
            }
        } else if (type == byte[].class) {
            byte[] array = (byte[]) object;
            output.writeInt(array.length);
            output.writeBytes(array);
        } else if (type == short[].class) {
            short[] array = (short[]) object;
            output.writeInt(array.length);
            for (short value : array) {
                output.writeShort(value);
            }
        } else if (type == char[].class) {
            char[] array = (char[]) object;
            output.writeInt(array.length);
            for (char value : array) {
                output.writeChar(value);
            }
        } 
        else if (type == Integer[].class) {
            Integer[] array = (Integer[]) object;
            output.writeInt(array.length);
            for (Integer value : array) {
                output.writeBoolean(value != null);
                if (value != null) {
                    output.writeInt(value);
                }
            }
        } else if (type == Long[].class) {
            Long[] array = (Long[]) object;
            output.writeInt(array.length);
            for (Long value : array) {
                output.writeBoolean(value != null);
                if (value != null) {
                    output.writeLong(value);
                }
            }
        } else if (type == Double[].class) {
            Double[] array = (Double[]) object;
            output.writeInt(array.length);
            for (Double value : array) {
                output.writeBoolean(value != null);
                if (value != null) {
                    output.writeDouble(value);
                }
            }
        } else if (type == Float[].class) {
            Float[] array = (Float[]) object;
            output.writeInt(array.length);
            for (Float value : array) {
                output.writeBoolean(value != null);
                if (value != null) {
                    output.writeFloat(value);
                }
            }
        } else if (type == Boolean[].class) {
            Boolean[] array = (Boolean[]) object;
            output.writeInt(array.length);
            for (Boolean value : array) {
                output.writeBoolean(value != null);
                if (value != null) {
                    output.writeBoolean(value);
                }
            }
        } else if (type == Byte[].class) {
            Byte[] array = (Byte[]) object;
            output.writeInt(array.length);
            for (Byte value : array) {
                output.writeBoolean(value != null);
                if (value != null) {
                    output.writeByte(value);
                }
            }
        } else if (type == Short[].class) {
            Short[] array = (Short[]) object;
            output.writeInt(array.length);
            for (Short value : array) {
                output.writeBoolean(value != null);
                if (value != null) {
                    output.writeShort(value);
                }
            }
        } else if (type == Character[].class) {
            Character[] array = (Character[]) object;
            output.writeInt(array.length);
            for (Character value : array) {
                output.writeBoolean(value != null);
                if (value != null) {
                    output.writeChar(value);
                }
            }
        } else {
            throw new UnsupportedOperationException("Unsupported array type: " + type);
        }
    }
    
    @Override
    public T read(Kryo kryo, Input input, Class<T> type) {
        int length = input.readInt();
        if (length == -1) {
            return null;
        }
        
        if (type == int[].class) {
            int[] result = new int[length];
            for (int i = 0; i < length; i++) {
                result[i] = input.readInt();
            }
            return (T) result;
        } else if (type == long[].class) {
            long[] result = new long[length];
            for (int i = 0; i < length; i++) {
                result[i] = input.readLong();
            }
            return (T) result;
        } else if (type == double[].class) {
            double[] result = new double[length];
            for (int i = 0; i < length; i++) {
                result[i] = input.readDouble();
            }
            return (T) result;
        } else if (type == float[].class) {
            float[] result = new float[length];
            for (int i = 0; i < length; i++) {
                result[i] = input.readFloat();
            }
            return (T) result;
        } else if (type == boolean[].class) {
            boolean[] result = new boolean[length];
            for (int i = 0; i < length; i++) {
                result[i] = input.readBoolean();
            }
            return (T) result;
        } else if (type == byte[].class) {
            byte[] result = new byte[length];
            input.readBytes(result);
            return (T) result;
        } else if (type == short[].class) {
            short[] result = new short[length];
            for (int i = 0; i < length; i++) {
                result[i] = input.readShort();
            }
            return (T) result;
        } else if (type == char[].class) {
            char[] result = new char[length];
            for (int i = 0; i < length; i++) {
                result[i] = input.readChar();
            }
            return (T) result;
        } 
        else if (type == Integer[].class) {
            Integer[] result = new Integer[length];
            for (int i = 0; i < length; i++) {
                boolean isNotNull = input.readBoolean();
                result[i] = isNotNull ? input.readInt() : null;
            }
            return (T) result;
        } else if (type == Long[].class) {
            Long[] result = new Long[length];
            for (int i = 0; i < length; i++) {
                boolean isNotNull = input.readBoolean();
                result[i] = isNotNull ? input.readLong() : null;
            }
            return (T) result;
        } else if (type == Double[].class) {
            Double[] result = new Double[length];
            for (int i = 0; i < length; i++) {
                boolean isNotNull = input.readBoolean();
                result[i] = isNotNull ? input.readDouble() : null;
            }
            return (T) result;
        } else if (type == Float[].class) {
            Float[] result = new Float[length];
            for (int i = 0; i < length; i++) {
                boolean isNotNull = input.readBoolean();
                result[i] = isNotNull ? input.readFloat() : null;
            }
            return (T) result;
        } else if (type == Boolean[].class) {
            Boolean[] result = new Boolean[length];
            for (int i = 0; i < length; i++) {
                boolean isNotNull = input.readBoolean();
                result[i] = isNotNull ? input.readBoolean() : null;
            }
            return (T) result;
        } else if (type == Byte[].class) {
            Byte[] result = new Byte[length];
            for (int i = 0; i < length; i++) {
                boolean isNotNull = input.readBoolean();
                result[i] = isNotNull ? input.readByte() : null;
            }
            return (T) result;
        } else if (type == Short[].class) {
            Short[] result = new Short[length];
            for (int i = 0; i < length; i++) {
                boolean isNotNull = input.readBoolean();
                result[i] = isNotNull ? input.readShort() : null;
            }
            return (T) result;
        } else if (type == Character[].class) {
            Character[] result = new Character[length];
            for (int i = 0; i < length; i++) {
                boolean isNotNull = input.readBoolean();
                result[i] = isNotNull ? input.readChar() : null;
            }
            return (T) result;
        } else {
            throw new UnsupportedOperationException("Unsupported array type: " + type);
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PrimitiveArraySerializer) {
            PrimitiveArraySerializer<?> other = (PrimitiveArraySerializer<?>) obj;
            return this.type.equals(other.type);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return type.hashCode();
    }
}

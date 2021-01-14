package distributed.cache.consistent.hashing;

import java.io.*;

public class SerializationUtils {

    public static byte[] serialize(Object objct) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutput objectOutput;
        try {
            objectOutput = new ObjectOutputStream(byteArrayOutputStream);
            objectOutput.writeObject(objct);
            objectOutput.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[]{};
    }

    public static Object deserialize(byte[] data) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        ObjectInput objectInput = null;
        try {
            if(byteArrayInputStream.available()>0) {
                objectInput = new ObjectInputStream(byteArrayInputStream);
                return objectInput.readObject();
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}

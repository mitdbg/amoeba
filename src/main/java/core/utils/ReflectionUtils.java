package core.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ReflectionUtils {

    public static Object invokeMethod(Object obj, String methodName,
                                      Object... args) {
        try {
            Method m;
            if (args != null) {
                Class<?>[] argClasses = new Class<?>[args.length];
                for (int i = 0; i < args.length; i++)
                    argClasses[i] = args[i].getClass();
                m = obj.getClass().getMethod(methodName, argClasses);
                return m.invoke(obj, args);
            } else {
                m = obj.getClass().getMethod(methodName);
                return m.invoke(obj);
            }
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Error!");
    }

    public static Class<?> getClass(String className) {
        try {
            if (className == null)
                return null;
            else
                return Class.forName(className);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in instantiating class: "
                    + className);
        }
    }

    public static Object getInstance(String className) {
        try {
            return Class.forName(className).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Error in instantiating class: " + className);
    }

    public static Object getInstance(String className, Class<?>[] argTypes,
                                     Object[] args) {
        try {
            return Class.forName(className).getConstructor(argTypes)
                    .newInstance(args);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Error in instantiating class: " + className);
    }
}

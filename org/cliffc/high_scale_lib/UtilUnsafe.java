package org.cliffc.high_scale_lib;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

class UtilUnsafe {
  static Unsafe getUnsafe() {
    // Not on bootckasspath
    if (NonBlockingHashMap.class.getClassLoader() != null) {
      try {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        return (Unsafe) f.get(NonBlockingHashMap.class);
      } catch (Exception e) {
        throw new RuntimeException("Could not obtain access to sun.misc.Unsafe", e);
      }
    }
    return Unsafe.getUnsafe();
  }
}

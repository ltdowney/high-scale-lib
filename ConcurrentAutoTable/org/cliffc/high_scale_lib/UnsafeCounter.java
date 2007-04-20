
package org.cliffc.high_scale_lib;
import sun.misc.Unsafe;
import java.lang.reflect.*;

public final class UnsafeCounter extends Counter {
  public String name() { return "Unsafe"; }
  private static final Unsafe _unsafe = Unsafe.getUnsafe();
  private static final long _cnt_offset;
  static {                      // <clinit>
    Field f = null;
    try { 
      f = UnsafeCounter.class.getDeclaredField("_cnt"); 
    } catch( java.lang.NoSuchFieldException e ) {
    } 
    _cnt_offset = _unsafe.objectFieldOffset(f);
  }

  private long _cnt;
  public long get(){ return _cnt; }
  public void add( long x ) { 
    long cnt=0;
    do { 
      cnt = _cnt;
    } while( !_unsafe.compareAndSwapLong(this,_cnt_offset,cnt,cnt+x) );
  }
}

//package org.cliffc.high_scale_lib;
import sun.misc.Unsafe;
import java.lang.reflect.*;

public final class UnsafeCounter extends Counter {
  public String name() { return "Unsafe"; }
  private static final Unsafe _unsafe = Unsafe.getUnsafe();
  private static final long CNT_OFFSET;
  static { {			// <clinit>
    Field f = null;
    try { 
      f = UnsafeCounter.class.getDeclaredField("_cnt"); 
    } catch( java.lang.NoSuchFieldException e ) {
      throw new Error(e);
    } 
    CNT_OFFSET = _unsafe.objectFieldOffset(f);
    }
  }

  private long _cnt;
  public long get(){ return _cnt; }
  public void add( final long x ) { 
    long cnt=0;
    do { 
      cnt = _cnt;
    } while( !_unsafe.compareAndSwapLong(this,CNT_OFFSET,cnt,cnt+x) );
  }
}

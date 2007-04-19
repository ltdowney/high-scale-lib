import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public final class LockCounter extends Counter {
  public String name() { return "Lock"; }
  private final ReentrantLock _lock = new ReentrantLock();
  private long _cnt;
  public long get(){ return _cnt; }
  public void add( long x ) { 
    _lock.lock();
    _cnt += x; 
    _lock.unlock();
  }
}

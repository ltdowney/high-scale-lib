/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
import org.cliffc.high_scale_lib.*;
import java.util.*;

public class nbsi_tester extends Thread {

  public static void main( String args[] ) throws Exception {
    NonBlockingSetInt nbsi = new NonBlockingSetInt(100);
    print(nbsi);

    if( nbsi.add(3) != true )
      throw new Exception("adding 3 but 3 should not exist");
    if( nbsi.add(4) != true )
      throw new Exception("adding 4 but 4 should not exist");
    print(nbsi);

    for( int i=0; i<100; i++ )
      if( nbsi.add(i) != (i != 3 && i != 4) )
        throw new Exception("adding "+i+" but it should not exist");
    print(nbsi);

    nbsi.remove(0);
    if( nbsi.size() != 100-1 )
      throw new Exception("size off after remove");

    // simple iterator test
    for( Object s : nbsi ) {
      System.out.print(s);
    }
    System.out.println();

    for (Iterator<Integer> i = nbsi.iterator(); i.hasNext(); ) {
      int s = i.next();         // auto-box/unbox!
      i.remove();
      System.out.print(s);
    }
    System.out.println();
    print(nbsi);

    nbsi.clear();
    print(nbsi);
  }

  static void print(NonBlockingSetInt nbsi) throws Exception {
    int size = nbsi.size();
    System.out.print("set["+size+"]=");
    if( (size==0) != nbsi.isEmpty() )
      throw new Exception("size="+size+", isEmpty="+nbsi.isEmpty());
    System.out.println(nbsi);
    //for( int i=0; i<100; i++ )
    //  if( nbsi.contains(i) )
    //    System.out.print(""+i+",");
    //System.out.println("}");
  }
}

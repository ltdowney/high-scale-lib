
/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * A simple project build routine.

 * I hate makefiles, and ANT uses the horrid XML so I'm doing this hack
 * instead.  Actually, I've used this technique before to great success.
 * I ought to make a Real Project out of it someday.
 *
 * What you see is the 'guts' of make/ANT PLUS the project file dependencies
 * at the end - all in pure Java.  Thus I can add new features to the make
 * (parallel distributed make, shared caching, etc) or add new dependencies.
 * Especially for the dependencies, there is no obtuse format to describe
 * things - it's all pure Java.
 *
 *
 * @since 1.5
 * @author Cliff Click
 */
class build { 
  static private class BuildError extends Error { BuildError( String s ) { super(s); } }

  static boolean _verbose;
  static boolean _justprint;
  static boolean _keepontrucking;
  // Top-level project directory
  static File TOP;
  static String TOP_PATH;
  static String TOP_PATH_SLASH;

  // --- main ----------------------------------------------------------------
  static public void main( String args[] ) throws IOException {
    // --- First up: find build.java!
    // Where we exec'd java.exe from
    //String cwd = new File( '.' ).getCanonicalPath();
    TOP = new File(".");
    TOP_PATH = TOP.getCanonicalPath();
    File f = new File(TOP,"build.java");
    while( !f.exists() ) {
      File p2 = new File(TOP,"..");
      if( p2.getCanonicalPath().equals(TOP_PATH) )
        throw new BuildError("build.java not found; build.java marks top of project heirarchy");
      TOP = p2;
      TOP_PATH = TOP.getCanonicalPath();
      f = new File(TOP,"build.java");
    }

    TOP_PATH_SLASH = TOP_PATH.replaceAll("\\\\","\\\\\\\\").concat("/");

    // --- Next up: always re-make self as needed
    if( _build.make() ) {
      // Since we remade ourself, launch & run self in a nested process to do
      // the actual 'build' using the new version of self.
      String a = "java build ";
      for( int i=0; i<args.length; i++ )
        a += args[i]+" ";
      sys_exec(a);
      System.exit(0);
    }

    // --- Strip out any flags; sanity check all targets before doing any of them
    int j = 0;
    boolean error = false;
    for( int i=0; i<args.length; i++ ) {
      if( args[i].charAt(0) == '-' ) {
        if( false ) ;
        else if( args[i].equals("-v") ) _verbose = true;
        else if( args[i].equals("-n") ) _justprint = true;
        else if( args[i].equals("-k") ) _keepontrucking = true;
        else {
          error = true;
          System.out.println("Unknown flag "+args[i]);
        }
      } else {
        if( Q.FILES.get(args[i]) == null ) {
          error = true;
          System.err.println("Unknown target "+args[i]);
        }
        args[j++] = args[i];
      }
    }
    if( error ) throw new Error("Command line errors");
    if( _verbose ) 
      System.out.println("Building in "+TOP.getCanonicalPath());

    // --- Build all named targets
    if( !_keepontrucking ) {
      // Build all till first error
      for( int i=0; i<j; i++ ) 
        Q.FILES.get(args[i]).make();
    } else {
      // Build all, keep going after failures
      try {
        for( int i=0; i<j; i++ ) 
          Q.FILES.get(args[i]).make();
      } catch( BuildError e ) {
        error = true;
        System.err.println(e);
      }
      if( error ) 
        throw new BuildError("Some build errors");
    }

    // --- All Done!
  }

  // --- StreamEater ---------------------------------------------------------
  static private class StreamEater extends Thread {
    final InputStream _is;
    IOException _e;
    StreamEater( InputStream is ) { _is = is; start(); }
    public void run() {
      int len;
      byte[] buf = new byte[1024];
      try {
        while( (len=_is.read(buf)) != -1 )
          System.out.write(buf,0,len);
      } catch( IOException e ) {
        _e = e;                 // Catch it for later, we're in the wrong thread
      }
    }
    public void close() throws IOException, InterruptedException {
      // called from the main thread on the StreamEater object, but not in the
      // StreamEater thread.
      join();
      if( _e != null ) throw _e; // Rethrow any exception in the main thread
    }
  }

  // --- sys_exec ------------------------------------------------------------
  // Run the command string as a new system process.  Throw an error if the
  // return value is not zero, or any number of other errors happen.
  static void sys_exec( String exec ) {
    if( _verbose ) System.out.println(exec);
    try {
      Process p = Runtime.getRuntime().exec(exec);
      StreamEater err = new StreamEater(p.getInputStream());
      StreamEater out = new StreamEater(p.getErrorStream());
      int status = p.waitFor();
      if( status != 0 ) 
        throw new BuildError("The completed '"+exec+"' returns status "+status);
      p.destroy();
      err.close();
      out.close();
    } catch( IOException e ) {
      throw new BuildError("Running '"+exec+"' threw IOException: "+e);
    } catch( InterruptedException e ) {
      throw new BuildError("Waiting on '"+exec+"' got interrupted: "+e);
    }
  }

  // --- A dependency --------------------------------------------------------
  static private class Q {

    // A table of all dependencies & target files in the world
    static ConcurrentHashMap<String,Q> FILES = new ConcurrentHashMap<String,Q>();

    // Basic definition of a dependency
    final String _target;
    final String _source;
    final String _exec;
    // These next fields are filled in lazily, as needed.
    String _parsed_exec;
    File _dst;                  // Actual OS files
    File _src;                  // Actual OS files
    long _dst_modtime;          // Actual (or effective mod time for '-n' builds)
    long _src_modtime;          // Actual (or effective mod time for '-n' builds)

    Q( String target, String source, String exec ) {
      // Basic sanity checking
      if( target.indexOf('%') != -1 ) 
        throw new IllegalArgumentException("dependency target has a '%': "+target);
      if( source.indexOf('%') != -1 ) 
        throw new IllegalArgumentException("dependency source has a '%': "+source);
      for( int i=exec.indexOf('%'); i!= -1; i = exec.indexOf('%',i+1) ) {
        if( false ) {
        } else if( exec.startsWith("src",i+1) ) { 
        } else if( exec.startsWith("dst",i+1) ) { 
        } else if( exec.startsWith("top",i+1) ) { 
        } else
          throw new IllegalArgumentException("dependency exec has unknown pattern: "+exec.substring(i));
      }
      // Fill in the fields
      _target = target;
      _source = source;
      _exec = exec;

      // Install the target/dependency mapping in a flat table
      if( FILES.put(target,this) != null ) 
        throw new IllegalArgumentException("More than one dependency for target "+target);
    }

    // --- parse_exec
    // The _exec String contains normal text, plus '%src' and '%dst' strings.
    String parse_exec() {
      if( _parsed_exec == null ) 
        _parsed_exec = _exec
          .replaceAll("%src",TOP_PATH_SLASH+_source)
          .replaceAll("%dst",TOP_PATH_SLASH+_target)
          .replaceAll("%top",TOP_PATH_SLASH);
      return _parsed_exec;
    }

    // --- make
    // Run the 'exec' string if the source file is more recent than the target file.
    // Return true if the target file (apparently) got updated.
    boolean make() {
      if( _dst == null ) { _dst = new File(TOP,_target); _dst_modtime = _dst.lastModified(); }
      if( _src == null ) { _src = new File(TOP,_source); _src_modtime = _src.lastModified(); }
      // Ugh, time only accurate to milli-seconds.  Build if times are equal,
      // because I can't tell who's on first.
      if( _dst_modtime > _src_modtime ) {
        if( _verbose ) System.out.println(_target+ " > " +_source + " : already up to date");
        return false;
      }

      // Files out of date; must do this build step
      String exec = parse_exec();
      if( _verbose ) System.out.println(_target+ " <= " +_source);
      if( _justprint ) {
        System.out.println(exec);
        _dst_modtime = _src_modtime+1; // Force modtime update
        return true;
      }
      
      // Actually do the step (make the sys-call)
      sys_exec(exec);

      // Double-check that this step made progress
      if( _src_modtime != _src.lastModified() )
        throw new IllegalArgumentException("Build step '"+exec+"' modified "+_source+" timestamp");
      long x = _dst.lastModified();
      if( _dst_modtime == x )
        throw new IllegalArgumentException("Build step '"+exec+"' did not modify "+_target+" timestamp");
      if( x <= _src_modtime )
        throw new IllegalArgumentException("Build step '"+exec+"' did not update "+_target+" timestamp past "+_source);
      
      return true;
    }

  };

  // =========================================================================
  // --- The Dependencies ----------------------------------------------------
  // =========================================================================

  static Q _build = new Q("build.class","build.java","javac %src");

  static Q _nbhm = new Q("org/cliffc/high_scale_lib/NonBlockingHashMap.class",
                         "org/cliffc/high_scale_lib/NonBlockingHashMap.java",
                         "javac -cp %top %src");
}

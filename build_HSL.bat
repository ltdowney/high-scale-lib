cd c:\Documents and Settings\Cliff\Desktop\Highly Scalable Java\high-scale-lib
javac -Xstdout Q -source 5 -target 5 -classpath org\cliffc\high_scale_lib org\cliffc\high_scale_lib\*.java
grep -v Unsafe Q
del Q
rem javadoc -package -link http://java.sun.com/j2se/1.5.0/docs/api *.java

rem cd ~/src/high-scale-lib; javac -source 5 -target 5 -cp . java/util/concurrent/ConcurrentHashMap.java; jar cf java_util_concurrent_chm.jar java/util/concurrent/*.class org/cliffc/high_scale_lib/*.class

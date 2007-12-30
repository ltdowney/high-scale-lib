cd c:\Documents and Settings\Cliff\Desktop\Highly Scalable Java\high-scale-lib
javac -Xstdout Q -source 5 -target 5 -classpath org\cliffc\high_scale_lib org\cliffc\high_scale_lib\*.java
grep -v Unsafe Q
del Q
rem javadoc -package -link http://java.sun.com/j2se/1.5.0/docs/api *.java

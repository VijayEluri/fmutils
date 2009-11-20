/**
 * <code>JWhich</code> is a utility that takes a Java class name
 * and displays the absolute pathname of the class file that would
 * be loaded first by the class loader, as prescribed by the
 * class path.
 * <p>
 * Usage is similar to the UNIX <code>which</code> command.
 * <p>
 * Example uses:
 * <p>
 * <blockquote>
 *      To find the absolute pathname of <code>MyClass.class</code>
 *      not in a package:
 *      <pre>java JWhich MyClass</pre>
 *
 *      To find the absolute pathname of <code>MyClass.class</code>
 *      in the <code>my.package</code> package:
 *      <pre>java JWhich my.package.MyClass</pre>
 * </blockquote>
 *
 * @author <a href="mailto:mike@clarkware.com">Mike Clark</a>
 * @author <a href="http://www.clarkware.com">Clarkware Consulting</a>
 */
package com.fmaritato;

import java.net.URL;

public class JWhich {

    /**
     * Prints the absolute pathname of the class file
     * containing the specified class name, as prescribed
     * by the current classpath.
     *
     * @param className Name of the class.
     */
    public static void which(String className) {
        URL classUrl = whichResrc(className);
        if (classUrl != null) {
            System.out.println("\nClass '" + className +
                               "' found in \n'" + classUrl.getFile() + "'");
        }
        else {
            System.out.println("\nClass '" + className +
                               "' not found in \n'" +
                               System.getProperty("java.class.path") + "'");
        }
    }

    public static URL whichResrc(String className) {

        if (!className.startsWith("/")) {
            className = "/" + className;
        }
        className = className.replace('.', '/');
        className = className + ".class";

        return JWhich.class.getResource(className);
    }

    public static void main(String args[]) {
        if (args.length > 0) {
            JWhich.which(args[0]);
        }
        else {
            System.err.println("Usage: java JWhich <classname>");
        }
    }
}

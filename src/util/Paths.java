package util;

import org.apache.hadoop.fs.Path;

import java.io.File;

public class Paths {
    public static boolean recursiveDeleteIfExists(Path p) {
        File file = new File(p.toString());
        if (file.exists()) {
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files != null) {
                    for (File f : files) {
                        recursiveDeleteIfExists(new Path(f.toString()));
                    }
                }
            }
            return file.delete();
        }
        return false;
    }

    public static Path of(String p) {
        return new Path(p);
    }
}

package com.surpass.util;

import java.io.File;

/**
 * Created by cwei@cz2r.com on 2016/12/9.
 */
public class FileUtil {

    private static boolean delDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = delDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }

    /**
     * 递归删除目录下的所有文件及子项目
     * @param path 子目录路径
     * @return
     */
    public static boolean delDir(String path) {
        File file = new File(path);
        return delDir(file);
    }
}

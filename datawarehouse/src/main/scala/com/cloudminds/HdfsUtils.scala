package com.cloudminds

import org.apache.hadoop.fs.{FileSystem, Path}

object HdfsUtils {
  /** 删除文件或目录 */
  def deletePath(file_sys: FileSystem, file: String): Boolean = {
    val path = new Path(file)
    if (file_sys.exists(path)) {
      file_sys.delete(path, true)
    } else{
      false
    }
  }

  /** hdfs文件或目录是否存在 */
  def isExists(file_sys: FileSystem, file: String): Boolean = file_sys.exists(new Path(file))
}

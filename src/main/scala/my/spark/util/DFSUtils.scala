package my.spark.util

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.IOException
import org.apache.hadoop.hdfs.DFSUtil
import org.apache.hadoop.fs.FsShell
import java.io.FileNotFoundException
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.hadoop.fs.permission.FsPermission

object DFSUtils {
  def rmr(pathStr: String, fs: FileSystem = FileSystem.get(new Configuration)) {
    val path = new Path(pathStr)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  def save(pathStr: String, str: String, append: Boolean = false, fs: FileSystem = FileSystem.get(new Configuration)) {

    val p = new Path(pathStr)
    var bf: BufferedWriter = null

    try {
      if (fs.exists(p)) {
        val status = fs.getFileStatus(p)
        status.getPath
        if (status.isFile()) {
          if (append) {
            bf = new BufferedWriter(new OutputStreamWriter(fs.append(p)))
          } else {
            bf = new BufferedWriter(new OutputStreamWriter(fs.create(p, true)))
          }
        } else {
          throw new IOException(s"Path ${status.getPath} exists but is a directory.")
        }
      } else {
        //        fs.mkdirs(p.getParent, new FsPermission("755"))
        bf = new BufferedWriter(new OutputStreamWriter(fs.create(p, true)))
      }

      bf.write(str)
      bf.flush()
    } finally {
      if (bf != null) bf.close()
    }
  }

  def read(pathStr: String, lineNum: Int, fs: FileSystem = FileSystem.get(new Configuration)) = {
    val p = new Path(pathStr)
    var br: BufferedReader = null
    try {
      br = new BufferedReader(new InputStreamReader(fs.open(p)))
      Array.tabulate(lineNum)(_ => br.readLine())
    } finally {
      if (br != null) br.close
    }

  }
}
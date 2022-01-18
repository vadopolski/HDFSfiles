package ru.otus.hadoop.sgribkov

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.net.URI
import org.apache.hadoop.fs.FileUtil._


object MoveHDFSfiles extends App {

  val conf = new Configuration()
  val fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  val sourceFolder = "/stage"
  val destinationFolder = "/ods"
  val sourceFolderPath = new Path(sourceFolder)
  val sourceSubFolders  = fs.listStatus(sourceFolderPath)

  sourceSubFolders.foreach(folder => {

    val sourceFolderPath = folder.getPath
    val destFolderPath = new Path(sourceFolderPath.toString.replace(sourceFolder, destinationFolder))

    if (!fs.exists(destFolderPath)) {
      fs.mkdirs(destFolderPath)
    }

    val files = fs
      .listStatus(sourceFolderPath)
      .map(_.getPath)
      .filter(_.getName.contains(".csv"))

    if (files.length > 0) {

      files.foreach(copy(fs, _, fs, destFolderPath, true, conf))

      val destFiles = files.map(f => new Path(destFolderPath + "/" + f.getName))
      val filesToConcat = destFiles.tail

      if (filesToConcat.length > 0){
        fs.concat(destFiles.head, destFiles.tail)
      }

    }

  })
  fs.close()

}

package com.collokia.kommon.jdk.files

import java.nio.file.Path
import java.nio.file.Files
import java.io.File

public fun Path.exists(): Boolean = Files.exists(this)
public fun Path.notExists(): Boolean = !this.exists()

public fun Path.deleteRecursive(): Unit = delDirRecurse(this.toFile())
public fun File.deleteRecursive(): Unit = delDirRecurse(this)

private fun delDirRecurse(f: File): Unit {
    if (f.isDirectory()) {
        for (c in f.listFiles()) {
            delDirRecurse(c)
        }
    }
    f.delete()
}
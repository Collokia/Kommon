package com.collokia.kommon.jdk.strings


public fun String.fromEnd(howManyFromEnd: Int): String = this.substring(this.length()-howManyFromEnd)
public fun String.fromStart(howManyFromStart: Int): String = this.substring(0, howManyFromStart)
public fun String.exceptEnding(allButThisMany: Int): String = this.substring(0, this.length()-allButThisMany)
public fun String.exceptLast(): String = this.substring(0, this.length()-1)
public fun String.exceptStarting(allAfterThisMany: Int): String = this.substring(allAfterThisMany)
public fun String.exceptFirst(): String = this.substring(1)

public fun String.mustStartWith(prefix: String): String {
  return if (this.startsWith(prefix)) {
    this
  }
  else {
    prefix + this
  }
}

public fun String.mustStartWith(prefix: Char): String {
  return if (this.startsWith(prefix)) {
    this
  }
  else {
    prefix + this
  }
}

public fun String.mustNotStartWith(prefix: String): String {
  return if (!this.startsWith(prefix)) {
    this
  }
  else {
    this.exceptStarting(prefix.length())
  }
}

public fun String.mustNotStartWith(prefix: Char): String {
  return if (!this.startsWith(prefix)) {
    this
  }
  else {
    this.exceptFirst()
  }
}


public fun String?.isNotTrimmedEmpty(): Boolean = (this ?: "").trim().isNotEmpty()
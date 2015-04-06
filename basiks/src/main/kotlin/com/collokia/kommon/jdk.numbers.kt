package com.collokia.kommon.jdk.numbers

public fun Int.minimum(minVal: Int): Int = Math.max(this, minVal)
public fun Int.maximum(maxVal: Int): Int = Math.min(this, maxVal)
public fun Int.coerce(minVal: Int, maxVal: Int): Int = this.minimum(minVal).maximum(maxVal)
public fun Int.coerce(range: IntRange): Int = this.minimum(range.start).maximum(range.end)

public fun Long.minimum(minVal: Long): Long = Math.max(this, minVal)
public fun Long.maximum(maxVal: Long): Long = Math.min(this, maxVal)
public fun Long.coerce(minVal: Long, maxVal: Long): Long = this.minimum(minVal).maximum(maxVal)
public fun Long.coerce(range: LongRange): Long = this.minimum(range.start).maximum(range.end)

public fun Byte.minimum(minVal: Byte): Byte = if (this < minVal) minVal else this
public fun Byte.maximum(maxVal: Byte): Byte = if (this > maxVal) maxVal else this
public fun Byte.coerce(minVal: Byte, maxVal: Byte): Byte = this.minimum(minVal).maximum(maxVal)
public fun Byte.coerce(range: ByteRange): Byte = this.minimum(range.start).maximum(range.end)

public fun Short.minimum(minVal: Short): Short = if (this < minVal) minVal else this
public fun Short.maximum(maxVal: Short): Short = if (this > maxVal) maxVal else this
public fun Short.coerce(minVal: Short, maxVal: Short): Short = this.minimum(minVal).maximum(maxVal)
public fun Short.coerce(range: ShortRange): Short = this.minimum(range.start).maximum(range.end)

public fun Double.minimum(minVal: Double): Double = Math.max(this, minVal)
public fun Double.maximum(maxVal: Double): Double = Math.min(this, maxVal)
public fun Double.coerce(minVal: Double, maxVal: Double): Double = this.minimum(minVal).maximum(maxVal)
public fun Double.coerce(range: DoubleRange): Double = this.minimum(range.start).maximum(range.end)
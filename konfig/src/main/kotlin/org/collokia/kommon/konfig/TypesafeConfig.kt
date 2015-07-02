package org.collokia.kommon.konfig

import com.typesafe.config.Config
import java.nio.file.Path
import java.nio.file.Paths

public fun Config.plus(fallback: Config): Config = this.withFallback(fallback)
public fun Config.value(key: String): ConfiguredValue = ConfiguredValue(this, key)
public fun Config.nested(key: String): Config = this.getConfig(key)
public fun Config.render(): String = this.root().render()

public class ConfiguredValue(val cfg: Config, val key: String) {
    fun asPath(): Path = Paths.get(cfg.getString(key).trim()).toAbsolutePath()
    fun asPathRelative(relativeTo: Path): Path = relativeTo.resolve(cfg.getString(key).trim()).toAbsolutePath()
    fun asPathSibling(relativeTo: Path): Path = relativeTo.resolveSibling(cfg.getString(key).trim()).toAbsolutePath()

    fun asString(): String = cfg.getString(key).trim()
    fun asString(defaultValue: String): String = if (exists()) cfg.getString(key).trim() else defaultValue
    fun asBoolean(): Boolean = cfg.getBoolean(key)
    fun asBoolean(defaultValue: Boolean): Boolean = if (exists()) cfg.getBoolean(key) else defaultValue
    fun asInt(): Int = cfg.getInt(key)
    fun asInt(defaultValue: Int): Int = if (exists()) cfg.getInt(key) else defaultValue
    fun asStringList(): List<String> = cfg.getStringList(key)
    fun asStringList(defaultValue: List<String>): List<String> = if (exists()) cfg.getStringList(key) else defaultValue
    fun asIntList(): List<Int> = cfg.getIntList(key)
    fun asIntList(defaultValue: List<Int>): List<Int> = if (exists()) cfg.getIntList(key) else defaultValue

    fun asStringArray(): Array<String> = cfg.getStringList(key).toTypedArray()
    fun asStringArray(defaultValue: Array<String>): Array<String> = if (exists()) cfg.getStringList(key).toTypedArray() else defaultValue

    fun asIntArray(): Array<Int> = cfg.getIntList(key).toTypedArray()
    fun asIntArray(defaultValue: Array<Int>): Array<Int> = if (exists()) cfg.getIntList(key).toTypedArray() else defaultValue

    fun asGuaranteedStringList(): List<String> = asStringList(emptyList())
    fun asGuaranteedIntList(): List<Int> = asIntList(emptyList())

    fun isZero(): Boolean = asInt() == 0

    fun isEmptyString(): Boolean = notExists() || asString().isEmpty()
    fun isNotEmptyString(): Boolean = exists() && asString().isNotEmpty()
    fun isBlankString(): Boolean = notExists() || asString().isNullOrBlank()
    fun isNotBlankString(): Boolean = exists() || asString().isNotBlank()

    fun exists(): Boolean = cfg.hasPath(key)
    fun notExists(): Boolean = !cfg.hasPath(key)
}

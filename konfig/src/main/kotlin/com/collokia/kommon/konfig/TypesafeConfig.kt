package com.collokia.kommon.konfig

import com.typesafe.config.Config
import java.nio.file.Path
import java.nio.file.Paths

public fun Config.plus(fallback: Config): Config = this.withFallback(fallback)
public fun Config.value(key: String): ConfiguredValue = ConfiguredValue(this, key)
public fun Config.nested(key: String): Config = this.getConfig(key)
public fun Config.render(): String = this.root().render()

public class ConfiguredValue(val cfg: Config, val key: String) {
    fun asPath(): Path = Paths.get(cfg.getString(key).trim()).toAbsolutePath()
    fun asPath(relativeTo: Path): Path = relativeTo.resolveSibling(cfg.getString(key).trim()).toAbsolutePath()

    fun asString(): String = cfg.getString(key).trim()
    fun asBoolean(): Boolean = cfg.getBoolean(key)
    fun asInt(): Int = cfg.getInt(key)
    fun asStringList(): List<String> = cfg.getStringList(key)
    fun asStringArray(): Array<String> = cfg.getStringList(key).copyToArray()
    fun asDefaultedStringList(default: List<String>): List<String> = if (exists()) asStringList() else default
    fun asGuaranteedStringList(): List<String> = if (exists()) asStringList() else listOf()

    fun isZero(): Boolean = asInt() == 0

    fun isEmptyString(): Boolean = notExists() || asString().isEmpty()
    fun isNotEmptyString(): Boolean = exists() && asString().isNotEmpty()

    fun exists(): Boolean = cfg.hasPath(key)
    fun notExists(): Boolean = !cfg.hasPath(key)
}

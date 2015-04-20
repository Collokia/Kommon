package com.collokia.kommon.json

import java.util.*


trait JsonElement {
    fun build(builder: StringBuilder)
}

public class JsonRoot internal (): JsonElement {
    companion object {
        private fun json(body: JsonRoot.() -> Unit) : JsonRoot {
            val json = JsonRoot()
            json.body()
            return json
        }

        public fun asArray(body: JsonArray.() -> Unit): JsonRoot {
           val json = json {
              jsonArray(body)
           }
            return json
        }

        public fun asObject(body: JsonObject.() -> Unit): JsonRoot {
            val json = json {
                jsonObject(body)
            }
            return json
        }
    }
    private var _element : JsonElement? = null
    fun set(element : JsonElement) { _element = element }
    override fun build(builder: StringBuilder) = _element?.build(builder)
    override fun toString(): String {
        val buffer = StringBuilder()
        build(buffer)
        return buffer.toString()
    }
}

class JsonArray internal () : JsonElement {
    private val elements = ArrayList<JsonElement>()

    fun add(value: JsonElement) = elements.add(value)

    override fun build(builder: StringBuilder) {
        builder.append("[")
        var first = true
        for(item in elements) {
            if (!first)
                builder.append(",")
            item.build(builder)
            first = false
        }
        builder.append("]")
    }
}

class JsonObject internal () : JsonElement {
    private val properties = LinkedHashMap<String, JsonElement>()
    fun put(name: String, value: JsonElement) = properties.put(name, value)

    override fun build(builder: StringBuilder) {
        builder.append("{")
        var first = true
        for((key, value) in properties) {
            if (!first)
                builder.append(",")
            builder.append("\"${key}\"")
            builder.append(":")
            value.build(builder)
            first = false
        }
        builder.append("}")
    }

    fun isEmpty(): Boolean {
        return properties.isEmpty()
    }
}


class JsonValue internal (val value: Any) : JsonElement {
    override fun build(builder: StringBuilder) {
        when (value) {
            is Int -> builder.append(value)
            is Long -> builder.append(value)
            is Boolean -> builder.append(value)
            else -> builder.append("\"${value}\"")
        }
    }
}

class JsonRawValue internal (val rawJson: String) : JsonElement {
    override fun build(builder: StringBuilder) {
        builder.append(rawJson)
    }
}

fun JsonArray.jsonValue(value: String) = add(jsonString(value))
fun JsonArray.jsonValue(value: Number) = add(JsonValue(value))
fun JsonArray.jsonValue(value: Boolean) = add(JsonValue(value))
fun JsonObject.jsonValue(name: String, value: String) = put(name, jsonString(value))
fun JsonObject.jsonValue(name: String, value: Number) = put(name, JsonValue(value))
fun JsonObject.jsonValue(name: String, value: Boolean) = put(name, JsonValue(value))
fun JsonObject.jsonValueAsRawJson(name: String, rawJson: String) = put(name, JsonRawValue(rawJson))

inline fun JsonArray.jsonObject(body: JsonObject.() -> Unit) {
    val value = JsonObject()
    value.body()
    add(value)
}

inline fun JsonArray.jsonArray(body: JsonArray.() -> Unit) {
    val value = JsonArray()
    value.body()
    add(value)
}

inline fun JsonObject.jsonObject(name: String, body: JsonObject.() -> Unit) {
    val value = JsonObject()
    value.body()
    put(name, value)
}

inline fun JsonObject.jsonArray(name: String, body: JsonArray.() -> Unit) {
    val value = JsonArray()
    value.body()
    put(name, value)
}

inline fun JsonRoot.jsonArray(body: JsonArray.() -> Unit) {
    val array = JsonArray()
    array.body()
    set(array)
}

inline fun JsonRoot.jsonObject(body: JsonObject.() -> Unit){
    val obj = JsonObject()
    obj.body()
    set(obj)
}

// TODO: is this even used?
inline fun jsonArray(body: JsonArray.() -> Unit) : JsonElement {
    val array = JsonArray()
    array.body()
    return array
}

// TODO: is this even used?
inline fun jsonObject(body: JsonObject.() -> Unit) : JsonElement {
    val obj = JsonObject()
    obj.body()
    return obj
}


// TODO: new escaper, this doesn't really cover the standard
//   escape ", \, / (missing), \b, \r, \n, \f, \t (escape dont convert to spaces)
internal fun jsonString(value: String): JsonValue = JsonValue(value.replace("\\", "\\\\").replace("/", "\\/").replace("\"", "\\\"").replace("\r\n", "\n").replace("\n", "\\n").replace("\t", "\\t"))



package com.collokia.kommon.jdk.uri

import com.collokia.kommon.jdk.collections.isNotEmpty
import com.collokia.kommon.jdk.strings.isNotTrimmedEmpty
import com.collokia.kommon.jdk.strings.mustNotStartWith
import com.collokia.kommon.jdk.strings.mustStartWith
import kotlin.properties.Delegates
import java.net.URI
import java.net.URL
import java.util.LinkedHashMap
import java.net.URLEncoder


public fun buildUri(uri: URI, init: UriBuilder.() -> Unit = {}): UriBuilder {
    return UriBuilder {
        scheme = uri.getScheme()
        userInfo = uri.getUserInfo()
        host = uri.getHost()
        port = uri.getPort()
        encodedPath = uri.getRawPath()
        query.putAll(queryStringToMap(uri.getRawQuery()))
        fragment = uri.getFragment()
        init()
    }
}

public fun buildUri(uriString: String, init: UriBuilder.() -> Unit = {}): UriBuilder {
    return buildUri(URI(uriString), init)
}

public fun buildUri(url: URL, init: UriBuilder.() -> Unit = {}): UriBuilder {
    return buildUri(url.toURI(), init)
}

public fun buildUri(uri: UriBuilder, init: UriBuilder.() -> Unit = {}): UriBuilder {
    return UriBuilder {
        scheme = uri.scheme
        userInfo = uri.userInfo
        host = uri.host
        port = uri.port
        encodedPath = uri.encodedPath
        query.putAll(uri.query)
        fragment = uri.fragment
        init()
    }
}

class UriBuilder(init: UriBuilder.() -> Unit = {}) {
    var scheme: String by Delegates.notNull()
    var userInfo: String? = null
    var host: String by Delegates.notNull()
    var port: Int? = null
    var encodedPath: String? = null
    val query: MutableMap<String, MutableList<String?>> = java.util.LinkedHashMap()
    var fragment: String? = null

    val decodedPath: String?
       get() = CorrectUrlDecoding.decodePath(encodedPath, "UTF-8")

    init {
        with(this) { init() }
    }

    fun UriBuilder.params(init: UriParams.() -> Unit = {}): Unit {
        UriParams().init()
    }

    inner class UriParams() {
        fun Pair<String, String?>.plus() {
            this@UriBuilder.withParams(this)
        }

        fun String.minus() {
            this@UriBuilder.removeParam(this)
        }
    }

    fun clearParams(): UriBuilder {
        query.clear()
        return this
    }

    fun clearQuery(): UriBuilder {
        return clearParams()
    }

    fun clearFragment(): UriBuilder {
        fragment = null
        return this
    }


    fun replaceParams(vararg params: Pair<String, String?>): UriBuilder {
        for (param in params) {
            query.remove(param.first)
        }
        return withParams(*params)
    }

    fun withParams(vararg params: Pair<String, String?>): UriBuilder {
        for (param in params) {
            query.getOrPut(param.first, { arrayListOf() }).add(param.second)
        }
        return this
    }

    fun removeParam(key: String): UriBuilder {
        query.remove(key)
        return this
    }

    fun setScheme(scheme: String): UriBuilder {
        this.scheme = scheme
        return this
    }
    fun setHost(host: String): UriBuilder {
        this.host = host
        return this
    }

    fun setEncodedPath(path: String): UriBuilder {
        this.encodedPath = path
        return this
    }

    fun setFragment(frag: String): UriBuilder {
        this.fragment = frag
        return this
    }

    fun build(): URI {
        val actualPort = port ?: -1
        val uri = URI(scheme, userInfo, host, actualPort, null, null, null)
        val sb = StringBuilder()
        if (encodedPath.isNotTrimmedEmpty()) {
            sb.append(encodedPath!!.mustStartWith('/'))
        }
        if (query.isNotEmpty()) {
            val queryString = queryMapToString(query)
            if (queryString.isNotTrimmedEmpty()) {
                sb.append('?').append(queryString!!.mustNotStartWith('?'))
            }
        }
        if (fragment.isNotTrimmedEmpty()) {
            sb.append('#').append(fragment!!.mustNotStartWith('#'))
        }
        return uri.resolve(sb.toString())
    }

    override fun toString(): String {
        return build().toString()
    }
}

private val utf8 = Charsets.UTF_8.name()

private fun queryStringToMap(queryString: String?): Map<String, MutableList<String?>> {
    val query = LinkedHashMap<String, MutableList<String?>>()
    if (queryString.isNotTrimmedEmpty()) {
        queryString?.split('&')?.forEach { keyValuePair ->
            val parts = keyValuePair.split("=", 2)
            val key = CorrectUrlDecoding.decodeQuery(parts[0].trim(), "UTF-8")
            val value = if (parts.size() == 2) CorrectUrlDecoding.decodeQuery(parts[1].trim(), "UTF-8") else null
            query.getOrPut(key, { arrayListOf() }).add(value)
        }
    }
    return query
}

private fun queryMapToString(queryString: Map<String, List<String?>>): String? {
    return if (queryString.isEmpty()) {
        null
    } else {
        val parts = queryString.entrySet().map { entry ->
            val encodedKey = URLEncoder.encode(entry.key, utf8)
            entry.value.map { value -> Pair<String, String?>(encodedKey, URLEncoder.encode(value ?: "", utf8)) }
        }.flatMap { it }

        parts.map { "${it.first}=${it.second}" }.joinToString("&")
    }
}



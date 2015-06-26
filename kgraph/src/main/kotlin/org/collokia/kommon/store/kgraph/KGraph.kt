package org.collokia.kommon.store.kgraph

import org.collokia.kommon.store.kgraph.internal.*
import com.netflix.nfgraph.NFGraph
import com.netflix.nfgraph.OrdinalIterator
import com.netflix.nfgraph.build.NFBuildGraph
import com.netflix.nfgraph.compressed.NFCompressedGraph
import com.netflix.nfgraph.spec.NFGraphSpec
import com.netflix.nfgraph.spec.NFNodeSpec
import com.netflix.nfgraph.spec.NFPropertySpec
import com.netflix.nfgraph.util.OrdinalMap
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStream
import java.io.OutputStream
import kotlin.platform.platformName
import kotlin.properties.Delegates
import kotlin.support.AbstractIterator



public inline fun <reified N : Enum<N>, reified R : Enum<R>> defineGraphSchema(defaultStructure: RelationStructure, init: GraphSchemaBuilder<N, R>.() -> Unit): CompiledGraphSchema<N, R> {
    val schema = GraphSchemaBuilder(javaClass<N>(), javaClass<R>(), defaultStructure)
    schema.init()
    return CompiledGraphSchema(schema)
}

public fun <N : Enum<N>, R : Enum<R>> constructGraph(schema: CompiledGraphSchema<N, R>, init: GraphBuilder<N, R>.() -> Unit): GraphBuilder<N,R> {
    val builder = GraphBuilder(schema)
    builder.init()
    return builder
}

public inline fun <reified N : Enum<N>, reified R : Enum<R>> useGraph(inputStream: InputStream, run: ReadOnlyGraph<N, R>.() -> Unit): ReadOnlyGraph<N, R> {
    val graph = ReadOnlyGraph<N,R>(javaClass<N>(), javaClass<R>(), inputStream)
    graph.run()
    return graph
}


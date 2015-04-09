package com.collokia.kommon.nfgraph

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
import kotlin.properties.Delegates
import kotlin.support.AbstractIterator

trait GraphNodeType {
    fun name(): String
}

trait GraphRelationType {
    fun name(): String
}

trait GraphRelationOptions {
    val flags: Int
}

enum class RelationCardinality(override val flags: Int) : GraphRelationOptions {
    SINGLE : RelationCardinality(com.netflix.nfgraph.spec.NFPropertySpec.SINGLE)
    MULTIPLE : RelationCardinality(com.netflix.nfgraph.spec.NFPropertySpec.MULTIPLE)
}

enum class RelationScope(override val flags: Int) : GraphRelationOptions {
    GLOBAL : RelationScope(com.netflix.nfgraph.spec.NFPropertySpec.GLOBAL)
    MODEL : RelationScope(com.netflix.nfgraph.spec.NFPropertySpec.MODEL_SPECIFIC)
}

enum class RelationStructure(override val flags: Int) : GraphRelationOptions {
    COMPACT : RelationStructure(com.netflix.nfgraph.spec.NFPropertySpec.COMPACT)
    HASH : RelationStructure(com.netflix.nfgraph.spec.NFPropertySpec.HASH)
}

// TODO: is this wording backwards?  ONE_TO_MANY is one forward connection, many backward connections
enum class BidirectionRelation(val fowardFlags: GraphRelationOptions, val backwardFlags: GraphRelationOptions) {
    ONE_TO_MANY : BidirectionRelation(RelationCardinality.SINGLE, RelationCardinality.MULTIPLE)
    MANY_TO_MANY : BidirectionRelation(RelationCardinality.MULTIPLE, RelationCardinality.MULTIPLE)
    MANY_TO_ONE : BidirectionRelation(RelationCardinality.MULTIPLE, RelationCardinality.SINGLE)
    ONE_TO_ONE : BidirectionRelation(RelationCardinality.SINGLE, RelationCardinality.SINGLE)
}

fun GraphRelationOptions.plus(other: GraphRelationOptions): GraphRelationOptions = TempGraphFlags(this.flags or other.flags)
fun GraphRelationOptions.minus(other: GraphRelationOptions): GraphRelationOptions = TempGraphFlags(this.flags and other.flags.inv())
class TempGraphFlags(override val flags: Int) : GraphRelationOptions

public class GraphSchema(private val defaultCardinality: BidirectionRelation = BidirectionRelation.MANY_TO_MANY, private val defaultStructure: RelationStructure = RelationStructure.HASH) {
    internal val nodeMap = hashMapOf<String, GraphNodeType>()
    internal val relations = linkedListOf<GraphRelationBuilder>()
    private val defaultScope = RelationScope.GLOBAL

    public fun GraphSchema.nodesFrom(nodes: Collection<out GraphNodeType>) {
        nodeMap.putAll(nodes.map { Pair(it.name(), it) })
    }

    public fun GraphSchema.nodesFrom(nodes: Array<out GraphNodeType>) {
        nodeMap.putAll(nodes.map { Pair(it.name(), it) })
    }

    public fun GraphSchema.from(nodeType: GraphNodeType): GraphRelationBuilder {
        nodeMap.put(nodeType.name(), nodeType)
        return GraphRelationBuilder(nodeType)
    }

    inner class GraphRelationBuilder(internal val fromNode: GraphNodeType) {
        internal var forwardRelation: GraphRelationType by Delegates.notNull()
        internal var forwardFlags: GraphRelationOptions = defaultScope + defaultStructure
        internal var backwardRelation: GraphRelationType? = null
        internal var backwardFlags: GraphRelationOptions = defaultScope + defaultStructure
        internal var toNode: GraphNodeType by Delegates.notNull()

        public fun edge(forwardRelation: GraphRelationType): GraphRelationPredicateEdge {
            this@GraphRelationBuilder.forwardRelation = forwardRelation
            return GraphRelationPredicateEdge()
        }

        inner class GraphRelationPredicateEdge() {
           public fun toOne(nodeType: GraphNodeType): GraphRelationPredicateNoBackwards {
               setToNode(nodeType)
               this@GraphRelationBuilder.forwardFlags = this@GraphRelationBuilder.forwardFlags - RelationCardinality.MULTIPLE + RelationCardinality.SINGLE
               return GraphRelationPredicateNoBackwards()
           }

            public fun toMany(nodeType: GraphNodeType): GraphRelationPredicateNoBackwards {
                setToNode(nodeType)
                this@GraphRelationBuilder.forwardFlags = this@GraphRelationBuilder.forwardFlags - RelationCardinality.SINGLE + RelationCardinality.MULTIPLE
                return GraphRelationPredicateNoBackwards()
            }

            private fun setToNode(nodeType: GraphNodeType) {
                this@GraphRelationBuilder.toNode = nodeType
                this@GraphSchema.nodeMap.put(nodeType.name(), nodeType)
                this@GraphSchema.relations.add(this@GraphRelationBuilder)
            }

           inner class GraphRelationPredicateNoBackwards {
                public fun globalScope(): GraphRelationPredicateNoBackwards {
                    this@GraphRelationBuilder.forwardFlags = this@GraphRelationBuilder.forwardFlags - RelationScope.MODEL + RelationScope.GLOBAL
                    return this
                }

                public fun modelScope(): GraphRelationPredicateNoBackwards {
                    this@GraphRelationBuilder.forwardFlags = this@GraphRelationBuilder.forwardFlags - RelationScope.GLOBAL + RelationScope.MODEL
                    return this
                }

                public fun compact(): GraphRelationPredicateNoBackwards {
                    this@GraphRelationBuilder.forwardFlags = this@GraphRelationBuilder.forwardFlags - RelationStructure.HASH + RelationStructure.COMPACT
                    return this
                }

                public fun hashed(): GraphRelationPredicateNoBackwards {
                    this@GraphRelationBuilder.forwardFlags = this@GraphRelationBuilder.forwardFlags - RelationStructure.COMPACT + RelationStructure.HASH
                    return this
                }

                public fun oneEdgeBack(backRelation: GraphRelationType): GraphRelationPredicateWithBackEdge {
                    this@GraphRelationBuilder.backwardRelation = backRelation
                    this@GraphRelationBuilder.backwardFlags = this@GraphRelationBuilder.backwardFlags - RelationCardinality.MULTIPLE + RelationCardinality.SINGLE
                    return GraphRelationPredicateWithBackEdge()
                }

                public fun manyEdgesBack(backRelation: GraphRelationType): GraphRelationPredicateWithBackEdge {
                    this@GraphRelationBuilder.backwardRelation = backRelation
                    this@GraphRelationBuilder.backwardFlags = this@GraphRelationBuilder.backwardFlags - RelationCardinality.SINGLE + RelationCardinality.MULTIPLE
                    return GraphRelationPredicateWithBackEdge()
                }

               inner class GraphRelationPredicateWithBackEdge {
                   public fun globalScope(): GraphRelationPredicateWithBackEdge {
                       this@GraphRelationBuilder.backwardFlags = this@GraphRelationBuilder.backwardFlags - RelationScope.MODEL + RelationScope.GLOBAL
                       return this
                   }

                   public fun modelScope(): GraphRelationPredicateWithBackEdge {
                       this@GraphRelationBuilder.backwardFlags = this@GraphRelationBuilder.backwardFlags - RelationScope.GLOBAL + RelationScope.MODEL
                       return this
                   }

                   public fun compact(): GraphRelationPredicateWithBackEdge {
                       this@GraphRelationBuilder.backwardFlags = this@GraphRelationBuilder.backwardFlags - RelationStructure.HASH + RelationStructure.COMPACT
                       return this
                   }

                   public fun hashed(): GraphRelationPredicateWithBackEdge {
                       this@GraphRelationBuilder.backwardFlags = this@GraphRelationBuilder.backwardFlags - RelationStructure.COMPACT + RelationStructure.HASH
                       return this
                   }
               }

            }
        }

    }

}

abstract class GraphOrdinalContainer(private val readOnlyOrdinals: Boolean) {
    protected val ordinalsByType: MutableMap<String, OrdinalMap<String>> = hashMapOf()

    protected fun nodeOrdMap(nodeTypeName: String): OrdinalMap<String> {
        return ordinalsByType.getOrPut(nodeTypeName, { OrdinalMap<String>() })
    }

    public fun ord(nodeType: GraphNodeType, primaryKey: String): Int {
        val nodeOrdinals = nodeOrdMap(nodeType.name())
        return if (readOnlyOrdinals) nodeOrdinals.get(primaryKey)
               else nodeOrdinals.add(primaryKey)
    }

    public fun GraphNodeType.withKey(id: String): Pair<GraphNodeType, Int> = Pair(this, ord(this, id))
    public fun GraphNodeType.invoke(id: String): Pair<GraphNodeType, Int> = Pair(this, ord(this, id))
    public fun GraphNodeType.get(id: String): Pair<GraphNodeType, Int> = Pair(this, ord(this, id))
    public fun GraphNodeType.withKeys(vararg id: String): Pair<GraphNodeType, Set<Int>> {
        return Pair(this, id.map { ord(this,it) }.toSet())
    }
}

public class GraphBuilder(val schema: GraphSchema) : GraphOrdinalContainer(false) {
    private data class UniDirectionalRelation(val fromNodeType: GraphNodeType, val relation: GraphRelationType, val toNodeType: GraphNodeType)

    private val relations = hashMapOf<String, MutableSet<UniDirectionalRelation>>()
    private val relationFlags = hashMapOf<UniDirectionalRelation, Int>()
    private val relationMirrors = hashMapOf<UniDirectionalRelation, UniDirectionalRelation>()

    private val graphSpec = run {
        val spec = NFGraphSpec()

        schema.relations.forEach { relate ->
            val forwardRelation = UniDirectionalRelation(relate.fromNode, relate.forwardRelation, relate.toNode)
            relations.getOrPut(relate.fromNode.name(), { hashSetOf() }).add(forwardRelation)
            relationFlags.put(forwardRelation, relate.forwardFlags.flags)
            if (relate.backwardRelation != null) {
                val backwardRelation = UniDirectionalRelation(relate.toNode, relate.backwardRelation!!, relate.fromNode)
                relations.getOrPut(relate.toNode.name(), { hashSetOf() }).add(backwardRelation)
                relationFlags.put(backwardRelation, relate.backwardFlags.flags)

                relationMirrors.put(forwardRelation, backwardRelation)
                relationMirrors.put(backwardRelation, forwardRelation)
            }
        }

        schema.nodeMap.values().forEach { node ->
            val nodeRelations: Set<UniDirectionalRelation> = relations.get(node.name()) ?: setOf()
            val propSpecs = nodeRelations.map { relate -> NFPropertySpec(relate.relation.name(), relate.toNodeType.name(), relationFlags.get(relate)) }.copyToArray()
            spec.addNodeSpec(NFNodeSpec(node.name(), *propSpecs))
        }
        spec
    }

    private val graphBuilder = NFBuildGraph(graphSpec)

    public fun connect(fromNode: Pair<GraphNodeType, Int>, relation: GraphRelationType, toNode: Pair<GraphNodeType, Int>) {
        val (fromNodeType, fromOrd) = fromNode
        val (toNodeType, toOrd) = toNode

        val nodeTypeRelations = relations.getOrElse(fromNodeType.name(), { setOf<GraphBuilder.UniDirectionalRelation>() })
        if (nodeTypeRelations.isNotEmpty()) {
            val matchingRelation = nodeTypeRelations.filter { it.relation.name() == relation.name() && it.toNodeType.name() == toNodeType.name() }.firstOrNull()
            if (matchingRelation != null) {
                graphBuilder.addConnection(matchingRelation.fromNodeType.name(), fromOrd, matchingRelation.relation.name(), toOrd)
                val mirrorRelation = relationMirrors.get(matchingRelation)
                if (mirrorRelation != null) {
                    graphBuilder.addConnection(mirrorRelation.fromNodeType.name(), toOrd, mirrorRelation.relation.name(), fromOrd)
                }
            } else {
                throw RuntimeException("Invalid relation type from node ${fromNodeType}, to node ${toNodeType}, of relation type ${relation}")
            }
        } else {
            throw RuntimeException("Node of type ${fromNodeType} has no relations, cannot connect it to anything!")
        }
    }

    public fun serialize(output: OutputStream) {
        DataOutputStream(output).use { dataStream ->

            // Header:
            //   "**KOTLIN-NFGRAPH**"
            //   version as number
            // Partial Schema: (only unidirectional relations, no mirrors)
            //   "**SCHEMA**"
            //   number of nodeType
            //   foreach nodeType
            //      - name of nodeType
            //      - number of relations
            //     foreach relation
            //        - name of relation
            //        - name of to nodeType
            //
            // Ordinals:
            //   "**ORDINALS**"
            //   number of ordinal maps
            //   foreach ordinal map
            //      - name of nodeType
            //      - count N of ordinals
            //      - 0..N of ordinals
            //
            // Graph:
            //   "**GRAPH**"
            //   compressed graph serialized

            // Header
            dataStream.writeUTF(GRAPH_MARKERS_HEADER)
            dataStream.writeInt(1)

            // Partial Schema:
            dataStream.writeUTF(GRAPH_MARKERS_SCHEMA_HEADER)
            val values = schema.nodeMap.values()
            dataStream.writeInt(values.size())
            values.forEach { nodeType ->
                dataStream.writeUTF(nodeType.name())
                val relats: Set<GraphBuilder.UniDirectionalRelation> = relations.get(nodeType.name()) ?: setOf()
                dataStream.writeInt(relats.size())
                relats.forEach { relation ->
                    dataStream.writeUTF(relation.relation.name())
                    dataStream.writeUTF(relation.toNodeType.name())
                }
            }

            // Ordinals:
            dataStream.writeUTF(GRAPH_MARKERS_ORDINAL_HEADER)
            dataStream.writeInt(ordinalsByType.size())
            ordinalsByType.forEach { entry ->
                dataStream.writeUTF(entry.getKey())
                dataStream.writeInt(entry.getValue().size())
                entry.getValue().iterator().forEach { ordValue ->
                    dataStream.writeUTF(ordValue)
                }
            }

            // Graph:
            dataStream.writeUTF(GRAPH_MARKERS_GRAPH_HEADER)
            val compressed = graphBuilder.compress()
            compressed.writeTo(dataStream)
        }
    }
}

private val GRAPH_MARKERS_HEADER = "**KOTLIN-NFGRAPH**"
private val GRAPH_MARKERS_SCHEMA_HEADER = "**SCHEMA**"
private val GRAPH_MARKERS_ORDINAL_HEADER = "**ORDINALS**"
private val GRAPH_MARKERS_GRAPH_HEADER = "**GRAPH**"

private fun checkHeaderValue(input: DataInputStream, testValue: String) {
    val actualValue = input.readUTF()
    if (actualValue != testValue) {
       throw RuntimeException("Check header value is not correct: ${testValue} vs. actual ${actualValue}")
    }
}

public class Graph(private val input: InputStream) : GraphOrdinalContainer(true) {
    protected var nodeTypeNames: Set<String> by Delegates.notNull()
    protected var relations: Map<Pair<String,String>, String> by Delegates.notNull()

    protected val graph: NFGraph = run {
        DataInputStream(input).use { dataStream ->
            // Header:
            checkHeaderValue(dataStream, GRAPH_MARKERS_HEADER)
            val version = dataStream.readInt() // TODO: if we need to check it for differences later

            // Partial Schema:
            checkHeaderValue(dataStream, GRAPH_MARKERS_SCHEMA_HEADER)
            val tempRelations = hashMapOf<Pair<String,String>, String>()
            val tempNodeTypes = hashSetOf<String>()

            val numberOfNodeTypes = dataStream.readInt()
            for (i in 1..numberOfNodeTypes) {
                val nodeTypeName = dataStream.readUTF()
                tempNodeTypes.add(nodeTypeName)
                val relationCount = dataStream.readInt()
                for (x in 1..relationCount) {
                    val relationName = dataStream.readUTF()
                    val toNodeTypeName = dataStream.readUTF()
                    tempRelations.put(Pair(nodeTypeName, relationName), toNodeTypeName)
                }
            }
            nodeTypeNames = tempNodeTypes
            relations = tempRelations

            // Ordinals:
            checkHeaderValue(dataStream, GRAPH_MARKERS_ORDINAL_HEADER)
            val numberOfOrdinalMaps = dataStream.readInt()
            for (i in 1..numberOfOrdinalMaps) {
                val nodeTypeName = dataStream.readUTF()
                val nodeOrdinalCount = dataStream.readInt()
                val nodeOrdMap = nodeOrdMap(nodeTypeName)
                for (x in 1..nodeOrdinalCount) {
                   val oneValue = dataStream.readUTF()
                    nodeOrdMap.add(oneValue)
                }
            }

            // Graph:
            checkHeaderValue(dataStream, GRAPH_MARKERS_GRAPH_HEADER)
            NFCompressedGraph.readFrom(dataStream)
        }
    }

    public class OrdinalIterable(private val iterator: OrdinalIterator): Iterable<Int> {
        override fun iterator(): Iterator<Int> {
            return OrdinalIteration(iterator)
        }
    }

    public class OrdinalIteration(private val iterator: OrdinalIterator) : AbstractIterator<Int>() {
        override fun computeNext() {
            val nextVal = iterator.nextOrdinal()
            if (nextVal != OrdinalIterator.NO_MORE_ORDINALS) {
                this.setNext(nextVal)
            }
            else {
                this.done()
            }
        }
    }

    protected fun relationTargetFor(fromNode: GraphNodeType, relation: GraphRelationType): String {
        val target = relations.get(Pair(fromNode.name(), relation.name()))
        if (target != null) {
            return target
        }
        throw RuntimeException("Unknown relationship from node ${fromNode} of type ${relation}")
    }

    protected fun NFGraph.connectionsAsIterator(nodeType: GraphNodeType, nodeOrd: Int, relation: GraphRelationType): OrdinalIteration = OrdinalIteration(this.getConnectionIterator(nodeType.name(),nodeOrd,relation.name()))
    protected fun NFGraph.connectionsAsIterable(nodeType: GraphNodeType, nodeOrd: Int, relation: GraphRelationType): OrdinalIterable = OrdinalIterable(this.getConnectionIterator(nodeType.name(),nodeOrd,relation.name()))
    protected fun NFGraph.connectionsAsSet(nodeType: GraphNodeType, nodeOrd: Int, relation: GraphRelationType): Set<Int> = OrdinalIterable(this.getConnectionIterator(nodeType.name(),nodeOrd,relation.name())).toSet()

    public fun connectionsFor(fromNode: Pair<GraphNodeType,Int>, relation: GraphRelationType): Set<String> {
        val ordsForTarget = nodeOrdMap(relationTargetFor(fromNode.first, relation))
        return graph.connectionsAsIterable(fromNode.first,fromNode.second,relation).map { ordsForTarget.get(it) }.toSet()
    }

    public fun connectionsContains(fromNode: Pair<GraphNodeType,Int>, relation: GraphRelationType, idOfTarget: String): Boolean {
        val ordsForTarget = nodeOrdMap(relationTargetFor(fromNode.first, relation))
        val ordOfTragetId = ordsForTarget.get(idOfTarget)
        return graph.getConnectionSet(fromNode.first.name(), fromNode.second, relation.name()).contains(ordOfTragetId)
    }

    public fun connectionsContainsAll(fromNode: Pair<GraphNodeType,Int>, relation: GraphRelationType, idsOfTarget: Set<String>): Boolean {
        val ordsForTarget = nodeOrdMap(relationTargetFor(fromNode.first, relation))
        val connectionSet =  graph.getConnectionSet(fromNode.first.name(), fromNode.second, relation.name())
        return idsOfTarget.all { connectionSet.contains(ordsForTarget.get(it)) }
    }

    public fun connectionsContainsOnly(fromNode: Pair<GraphNodeType,Int>, relation: GraphRelationType, idsOfTarget: Set<String>): Boolean {
        val ordsForTarget = nodeOrdMap(relationTargetFor(fromNode.first, relation))
        val connectionSet =  graph.getConnectionSet(fromNode.first.name(), fromNode.second, relation.name())
        return connectionSet.size() == idsOfTarget.size() && idsOfTarget.all { connectionSet.contains(ordsForTarget.get(it)) }
    }

    public fun connectionsContainsAny(fromNode: Pair<GraphNodeType,Int>, relation: GraphRelationType, idsOfTarget: Set<String>): Boolean {
        val ordsForTarget = nodeOrdMap(relationTargetFor(fromNode.first, relation))
        val connectionSet =  graph.getConnectionSet(fromNode.first.name(), fromNode.second, relation.name())
        return idsOfTarget.any { connectionSet.contains(ordsForTarget.get(it)) }
    }

    protected fun validateTargetNodeType(fromNode: GraphNodeType, relation: GraphRelationType, targetNode: GraphNodeType) {
        if (relationTargetFor(fromNode, relation) != targetNode.name()) {
            throw RuntimeException("Checking incorrect target node type for relation, from ${fromNode} of relation ${relation} to ${targetNode}")
        }
    }

    public fun connectionsContains(fromNode: Pair<GraphNodeType,Int>, relation: GraphRelationType, targetNode: Pair<GraphNodeType,Int>): Boolean {
        validateTargetNodeType(fromNode.first,relation,targetNode.first)
        return graph.getConnectionSet(fromNode.first.name(), fromNode.second, relation.name()).contains(targetNode.second)
    }

    public fun connectionsContainsAll(fromNode: Pair<GraphNodeType,Int>, relation: GraphRelationType, targetNodes: Pair<GraphNodeType, Set<Int>>): Boolean {
        validateTargetNodeType(fromNode.first,relation,targetNodes.first)
        val connectionSet =  graph.getConnectionSet(fromNode.first.name(), fromNode.second, relation.name())
        return targetNodes.second.all { connectionSet.contains(it) }
    }

    public fun connectionsContainsOnly(fromNode: Pair<GraphNodeType,Int>, relation: GraphRelationType, targetNodes: Pair<GraphNodeType, Set<Int>>): Boolean {
        validateTargetNodeType(fromNode.first,relation,targetNodes.first)
        val connectionSet =  graph.getConnectionSet(fromNode.first.name(), fromNode.second, relation.name())
        return connectionSet.size() == targetNodes.second.size() && targetNodes.second.all { connectionSet.contains(it) }
    }

    public fun connectionsContainsAny(fromNode: Pair<GraphNodeType,Int>, relation: GraphRelationType, targetNodes: Pair<GraphNodeType, Set<Int>>): Boolean {
        validateTargetNodeType(fromNode.first,relation,targetNodes.first)
        val connectionSet =  graph.getConnectionSet(fromNode.first.name(), fromNode.second, relation.name())
        return targetNodes.second.any { connectionSet.contains(it) }
    }

}

public fun defineGraphSchema(defaultRelationCardinality: BidirectionRelation, defaultStructure: RelationStructure, init: GraphSchema.() -> Unit): GraphSchema {
    val schema = GraphSchema(defaultRelationCardinality, defaultStructure)
    schema.init()
    return schema
}

public fun constructGraph(schema: GraphSchema, init: GraphBuilder.() -> Unit = {}): GraphBuilder {
    val builder = GraphBuilder(schema)
    builder.init()
    return builder
}


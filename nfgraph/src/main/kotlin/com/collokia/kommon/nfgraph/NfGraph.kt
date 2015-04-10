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
import kotlin.platform.platformName
import kotlin.properties.Delegates
import kotlin.support.AbstractIterator

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

trait GraphRelationOptions {
    val flags: Int
}

internal fun GraphRelationOptions.plus(other: GraphRelationOptions): GraphRelationOptions = TempGraphFlags(this.flags or other.flags)
internal fun GraphRelationOptions.minus(other: GraphRelationOptions): GraphRelationOptions = TempGraphFlags(this.flags and other.flags.inv())
internal class TempGraphFlags(override val flags: Int) : GraphRelationOptions

public class GraphSchemaBuilder<N : Enum<N>, R : Enum<R>>(internal val nodeTypeEnum: Class<N>, internal val relationTypeEnum: Class<R>, private val defaultStructure: RelationStructure = RelationStructure.HASH) {
    internal val nodeTypes: Set<N> = nodeTypeEnum.getEnumConstants().toSet()
    internal val relationTypes: Set<R> = relationTypeEnum.getEnumConstants().toSet()

    internal val relations = linkedListOf<GraphRelationBuilder<N, R>>()

    // from(MyNode).manyEdges(Relation).to(OtherNode).mirrorOneEdge(ParentRelation)

    // TODO: maybe allow syntax similar top this notation below
    //    Tag[TaggedItem] = Post[Tagged]
    //    Tag[TaggedItem] = Answer[Tagged]
    //    Tag[compact(TaggedItem)] = Answer[hashed(Tagged)]
    //    Tag[compact(one(TaggedItem))] = Answer[hashed(many(Tagged))]
    //    Post[many(Answer)] = Answer[one(Post)]
    //    modelScope {
    //       ...more relations
    //    }
    //    globalScope {  // default without a scope
    //       ...more relatinos
    //    }
    //

    // so from that list, probably best is:
    //    Tag[TaggedItem] = Post[Tagged]
    //    Tag[TaggedItem] = Answer[Tagged]
    //    Post[many(Answer)] = Answer[one(Post)]
    //
    //       Tag    -(*)> TaggedItem  -> Post
    //       Post   -(*)> Tagged      -> Tag
    //       Tag    -(*)> TaggedItem  -> Answer
    //       Answer -(*)> Tagged      -> Tag
    //       Post   -(*)> Answer      -> Answer
    //       Answer -(1)> Post        -> Post
    //
    // and since these are ambiguous the end up as
    //
    //       Tag    -(*)> TaggedItem.Post    -> Post
    //       Post   -(*)> Tagged             -> Tag
    //       Tag    -(*)> TaggedItem.Answer  -> Answer
    //       Answer -(*)> Tagged             -> Tag
    //       Post   -(*)> Answer             -> Answer
    //       Answer -(1)> Post               -> Post
    //
    //     with relation group:
    //       TaggedItem(*) = setOf(TaggedItem.Post(*), TaggedItem.Answer(*))
    //


    public fun from(nodeType: N): GraphRelationBuilder<N, R> {
        return GraphRelationBuilder(relations, defaultStructure, nodeType, RelationScope.GLOBAL, null)
    }

    public fun modelScope(init: GraphScopeModel.() -> Unit) {
        val scope = GraphScopeModel()
        scope.init()
    }

    inner class GraphScopeModel() {
        public fun from(nodeType: N): GraphRelationBuilder<N, R> {
            return GraphRelationBuilder(relations, defaultStructure, nodeType, RelationScope.MODEL, null)
        }
    }
}

internal class GraphRelationBuilder<N : Enum<N>, R : Enum<R>>(internal val relations: MutableList<GraphRelationBuilder<N, R>>,
                                                              private val defaultStructure: RelationStructure,
                                                              internal val fromNode: N, internal val scopeAs: RelationScope,
                                                              internal val modelScopeName: String? = null) {
    internal var forwardRelation: R by Delegates.notNull()
    internal var forwardFlags: GraphRelationOptions = scopeAs + defaultStructure
    internal var backwardRelation: R? = null
    internal var backwardFlags: GraphRelationOptions = scopeAs + defaultStructure
    internal var toNode: N by Delegates.notNull()

    public fun oneEdge(relation: R): GraphRelationPredicateEdge<N, R> {
        forwardRelation = relation
        forwardFlags = forwardFlags - RelationCardinality.MULTIPLE + RelationCardinality.SINGLE
        return GraphRelationPredicateEdge(this)
    }

    public fun manyEdges(relation: R): GraphRelationPredicateEdge<N, R> {
        forwardRelation = relation
        forwardFlags = this@GraphRelationBuilder.forwardFlags - RelationCardinality.SINGLE + RelationCardinality.MULTIPLE
        return GraphRelationPredicateEdge(this)
    }

    internal fun completeEnough() {
        relations.add(this)
    }
}

internal class GraphRelationPredicateEdge<N : Enum<N>, R : Enum<R>>(private val builder: GraphRelationBuilder<N, R>) {
    public fun target(nodeType: N): GraphRelationPredicateNoBackwards<N, R> {
        builder.toNode = nodeType
        builder.completeEnough()
        return GraphRelationPredicateNoBackwards<N, R>(builder)
    }
}

internal class GraphRelationPredicateNoBackwards<N : Enum<N>, R : Enum<R>>(private val builder: GraphRelationBuilder<N, R>) {
    public fun globalScope(): GraphRelationPredicateNoBackwards<N, R> {
        builder.forwardFlags = builder.forwardFlags - RelationScope.MODEL + RelationScope.GLOBAL
        return this
    }

    public fun modelScope(): GraphRelationPredicateNoBackwards<N, R> {
        builder.forwardFlags = builder.forwardFlags - RelationScope.GLOBAL + RelationScope.MODEL
        return this
    }

    public fun compact(): GraphRelationPredicateNoBackwards<N, R> {
        builder.forwardFlags = builder.forwardFlags - RelationStructure.HASH + RelationStructure.COMPACT
        return this
    }

    public fun hashed(): GraphRelationPredicateNoBackwards<N, R> {
        builder.forwardFlags = builder.forwardFlags - RelationStructure.COMPACT + RelationStructure.HASH
        return this
    }

    public fun mirrorOneEdge(backRelation: R): GraphRelationPredicateWithBackEdge<N, R> {
        builder.backwardRelation = backRelation
        builder.backwardFlags = builder.backwardFlags - RelationCardinality.MULTIPLE + RelationCardinality.SINGLE
        return GraphRelationPredicateWithBackEdge(builder)
    }

    public fun mirrorManyEdges(backRelation: R): GraphRelationPredicateWithBackEdge<N, R> {
        builder.backwardRelation = backRelation
        builder.backwardFlags = builder.backwardFlags - RelationCardinality.SINGLE + RelationCardinality.MULTIPLE
        return GraphRelationPredicateWithBackEdge(builder)
    }
}

internal class GraphRelationPredicateWithBackEdge<N : Enum<N>, R : Enum<R>>(private val builder: GraphRelationBuilder<N, R>) {
    public fun globalScope(): GraphRelationPredicateWithBackEdge<N, R> {
        builder.backwardFlags = builder.backwardFlags - RelationScope.MODEL + RelationScope.GLOBAL
        return this
    }

    public fun modelScope(): GraphRelationPredicateWithBackEdge<N, R> {
        builder.backwardFlags = builder.backwardFlags - RelationScope.GLOBAL + RelationScope.MODEL
        return this
    }

    public fun compact(): GraphRelationPredicateWithBackEdge<N, R> {
        builder.backwardFlags = builder.backwardFlags - RelationStructure.HASH + RelationStructure.COMPACT
        return this
    }

    public fun hashed(): GraphRelationPredicateWithBackEdge<N, R> {
        builder.backwardFlags = builder.backwardFlags - RelationStructure.COMPACT + RelationStructure.HASH
        return this
    }
}


abstract class GraphOrdinalContainer<N : Enum<N>>(private val readOnlyOrdinals: Boolean) {
    protected val ordinalsByType: MutableMap<N, OrdinalMap<String>> = hashMapOf()
    /*
    protected val typeNameToEnum: Map<String, N> = nodeTypeEnum.getEnumConstants().map { Pair(it.name(), it) }.toMap()

    [deprecated("Try to always use enum instead of strings")]
    protected fun nodeOrdMap(nodeTypeName: String): OrdinalMap<String> {
        val nodeType = typeNameToEnum.get(nodeTypeName)
        if (nodeType == null) {
            throw RuntimeException("NodeType name ${nodeTypeName} does not exist in the enumeration for node types")
        }
        return nodeOrdMap(nodeType)
    }
    */

    protected fun nodeOrdMap(nodeType: N): OrdinalMap<String> {
        return ordinalsByType.getOrPut(nodeType, { OrdinalMap<String>() })
    }

    public fun ord(nodeType: N, primaryKey: String): Int {
        val nodeOrdinals = nodeOrdMap(nodeType)
        return if (readOnlyOrdinals) nodeOrdinals.get(primaryKey)
        else nodeOrdinals.add(primaryKey)
    }

    public fun N.withKey(id: String): NodeAndOrd<N> = NodeAndOrd(this, ord(this, id))
    public fun N.get(id: String): NodeAndOrd<N> = NodeAndOrd(this, ord(this, id))
    public fun N.get(vararg id: String): NodeAndOrdList<N> {
        return NodeAndOrdList<N>(this, id.map { ord(this, it) })
    }

    public fun N.withKeySet(vararg id: String): NodeAndOrdSet<N> {
        return NodeAndOrdSet<N>(this, id.map { ord(this, it) }.toSet())
    }

    public fun N.withKeys(vararg id: String): NodeAndOrdList<N> {
        return NodeAndOrdList<N>(this, id.map { ord(this, it) })
    }
}


data class NodeAndOrd<N : Enum<N>>(val nodeType: N, val ord: Int)
data class NodeAndOrdList<N : Enum<N>>(val nodeType: N, val ordList: List<Int>)
data class NodeAndOrdSet<N : Enum<N>>(val nodeType: N, val ordSet: Set<Int>)

data class NodeAndId<N : Enum<N>>(val nodeType: N, val id: String)
data class NodeAndIdList<N : Enum<N>>(val nodeType: N, val idList: List<String>)
data class NodeAndIdSet<N : Enum<N>>(val nodeType: N, val idSet: Set<String>)

data class RelationshipPairKey<N : Enum<N>, R : Enum<R>>(val fromNode: N, val relationship: R)
data class RelationshipTrippleKey<N : Enum<N>, R : Enum<R>>(val fromNode: N, val relationship: R, val toNode: N)

public class CompiledGraphSchema<N : Enum<N>, R : Enum<R>>(val schema: GraphSchemaBuilder<N, R>) {
    internal val nodeTypes: Set<N> = schema.nodeTypes
    internal val relationTypes: Set<R> = schema.relationTypes
    internal val nodeTypeEnum: Class<N> = schema.nodeTypeEnum
    internal val relationTypeEnum: Class<R> = schema.relationTypeEnum

    // when have a relationship of a->R->b and a->R->c need to change to a->R.b->b and a->R.c->c but later find both if someone asks for R again during querying

    // ok, take the schema, render out both the NFGraphSpec and also our version of this that we can serialize
    // Ours includes:
    //      outbound relationships for a given node type
    //      relationship groups that disambiguate any collisions
    //      relationship mirrors that automatically apply the reverse relationship when the opposite is added

    internal val relationshipGroups: MutableMap<RelationshipPairKey<N, R>, MutableSet<RelationshipTrippleKey<N, R>>> = hashMapOf()
    internal val relationshipMirrors = hashMapOf<RelationshipTrippleKey<N, R>, RelationshipTrippleKey<N, R>>()

    internal val graphSpec = NFGraphSpec()

    init {
        val relationshipFlags = hashMapOf<RelationshipTrippleKey<N, R>, Int>()

        fun createRelation(fromNode: N, relationship: R, toNode: N, relationOptions: GraphRelationOptions, modelScope: String? = null): RelationshipTrippleKey<N, R> {
            val trippleKey = RelationshipTrippleKey(fromNode, relationship, toNode)
            val oldFlags = relationshipFlags.get(trippleKey)
            if (oldFlags != null && oldFlags != relationOptions.flags) {
                throw RuntimeException("Repeated definition of a relationship must have the same flags: ${trippleKey} with old ${oldFlags} new ${relationOptions.flags}")
            }
            val pairKey = RelationshipPairKey(fromNode, relationship)
            val relationGroup = relationshipGroups.getOrPut(pairKey, { hashSetOf() })
            relationGroup.add(trippleKey)
            relationshipFlags.put(trippleKey, relationOptions.flags)
            return trippleKey
        }

        schema.relations.forEach { r ->
            val modelScope = if (r.scopeAs == RelationScope.GLOBAL) null else r.modelScopeName
            val forward = createRelation(r.fromNode, r.forwardRelation, r.toNode, r.forwardFlags, modelScope)
            if (r.backwardRelation != null) {
                val backward = createRelation(r.toNode, r.backwardRelation!!, r.fromNode, r.backwardFlags, modelScope)

                val oldForwardMirror = relationshipMirrors.get(forward)
                val oldBackwardMirror = relationshipMirrors.get(backward)

                if (oldForwardMirror != null && oldBackwardMirror != null && oldForwardMirror != backward && oldBackwardMirror != forward) {
                    throw RuntimeException("Repeated definition of a relationship mirrors must have the same mirror relationships: ${forward} and ${backward} have differing mirrors ${oldBackwardMirror} and ${oldForwardMirror}")
                }

                relationshipMirrors.put(forward, backward)
                relationshipMirrors.put(backward, forward)
            }
        }

        val relationshipGroupsPerNodeType = relationshipGroups.keySet().groupBy { it.fromNode }
        nodeTypes.forEach { node ->
            val nodeRelationGroups = relationshipGroupsPerNodeType.get(node) ?: listOf()
            val propSpecs = arrayListOf<NFPropertySpec>()

            if (nodeRelationGroups.isNotEmpty()) {
                nodeRelationGroups.forEach { pairKey ->
                    val groupMembers = relationshipGroups.getOrElse(pairKey, { hashSetOf() })
                    if (groupMembers.isNotEmpty()) {
                        val fullyQualify = groupMembers.size() > 1
                        groupMembers.forEach { trippleKey ->
                            val relateName = if (fullyQualify) "${trippleKey.relationship.name()}.${trippleKey.toNode.name()}" else trippleKey.relationship.name()
                            propSpecs.add(NFPropertySpec(relateName, trippleKey.toNode.name(), relationshipFlags.get(trippleKey)))
                        }
                    }
                }
                graphSpec.addNodeSpec(NFNodeSpec(node.name(), *(propSpecs.copyToArray())))
            }
        }
    }
}

public class GraphBuilder<N : Enum<N>, R : Enum<R>>(val schema: CompiledGraphSchema<N, R>) : GraphOrdinalContainer<N>(false) {
    private val graphBuilder = NFBuildGraph(schema.graphSpec)

    public fun connect(fromNodeWithOrd: NodeAndOrd<N>, relation: R, toNodeWithOrd: NodeAndOrd<N>) {
        val forward = connectInternal(fromNodeWithOrd, relation, toNodeWithOrd)

        val mirrorRelation = schema.relationshipMirrors.get(forward)
        if (mirrorRelation != null) {
            connectInternal(toNodeWithOrd, mirrorRelation.relationship, fromNodeWithOrd)
        }
    }

    private fun connectInternal(fromNodeWithOrd: NodeAndOrd<N>, relation: R, toNodeWithOrd: NodeAndOrd<N>): RelationshipTrippleKey<N,R> {
        val pairKey = RelationshipPairKey(fromNodeWithOrd.nodeType, relation)
        val trippleKey = RelationshipTrippleKey(fromNodeWithOrd.nodeType, relation, toNodeWithOrd.nodeType)

        val nodeRelations = schema.relationshipGroups.getOrElse(pairKey, { setOf<RelationshipTrippleKey<N,R>>() })
        if (nodeRelations.isNotEmpty()) {
            val fullyQualify = nodeRelations.size() > 1
            val matchingRelation = nodeRelations.filter { it == trippleKey }.firstOrNull()
            if (matchingRelation != null) {
                val relateName = if (fullyQualify) "${trippleKey.relationship.name()}.${trippleKey.toNode.name()}" else trippleKey.relationship.name()
                graphBuilder.addConnection(matchingRelation.fromNode.name(), fromNodeWithOrd.ord, relateName, toNodeWithOrd.ord)
                return trippleKey
            } else {
                throw RuntimeException("No relationship for ${trippleKey} exists, cannot connect these nodes!")
            }
        } else {
            throw RuntimeException("No relationship for ${pairKey} exists, cannot connect it to anything!")
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
            //   [foreach nodeType]
            //      - name of nodeType
            //   number of relation groups
            //   [foreach relation group]
            //      - nodeType name
            //      - relation name
            //      - number of targets
            //      [foreach target]
            //         - target nodeType name
            //   number of mirrors
            //   [foreach mirror]
            //      - forward nodeType name
            //      - forward relation name
            //      - forward target nodeType name
            //      - backward nodeType name
            //      - backward relation name
            //      - backward target nodeType name
            //
            // Ordinals:
            //   "**ORDINALS**"
            //   number of ordinal maps
            //   [foreach ordinal map]
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
            val values = schema.nodeTypes
            dataStream.writeInt(values.size())
            values.forEach { nodeType ->
                dataStream.writeUTF(nodeType.name())
            }
            val groups = schema.relationshipGroups
            dataStream.writeInt(groups.size())
            groups.entrySet().forEach { groupEntry ->
                dataStream.writeUTF(groupEntry.getKey().fromNode.name())
                dataStream.writeUTF(groupEntry.getKey().relationship.name())
                dataStream.writeInt(groupEntry.getValue().size())
                groupEntry.getValue().forEach { target ->
                    dataStream.writeUTF(target.toNode.name())
                }
            }
            val mirrors = schema.relationshipMirrors
            dataStream.writeInt(mirrors.size())
            mirrors.entrySet().forEach { mirrorEntry ->
                dataStream.writeUTF(mirrorEntry.getKey().fromNode.name())
                dataStream.writeUTF(mirrorEntry.getKey().relationship.name())
                dataStream.writeUTF(mirrorEntry.getKey().toNode.name())
                dataStream.writeUTF(mirrorEntry.getValue().fromNode.name())
                dataStream.writeUTF(mirrorEntry.getValue().relationship.name())
                dataStream.writeUTF(mirrorEntry.getValue().toNode.name())
            }

            // Ordinals:
            dataStream.writeUTF(GRAPH_MARKERS_ORDINAL_HEADER)
            dataStream.writeInt(ordinalsByType.size())
            ordinalsByType.forEach { entry ->
                dataStream.writeUTF(entry.getKey().name())
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

public class ReadOnlyGraph<N : Enum<N>, R : Enum<R>>(private val nodeTypeEnum: Class<N>, private val relationTypeEnum: Class<R>, private val input: InputStream) : GraphOrdinalContainer<N>(true) {
    protected val relationshipGroups: MutableMap<RelationshipPairKey<N, R>, MutableSet<RelationshipTrippleKey<N, R>>> = hashMapOf()
    protected val relationshipMirrors: MutableMap<RelationshipTrippleKey<N, R>, RelationshipTrippleKey<N, R>> = hashMapOf()
    internal val nodeTypes: Map<String,N> = nodeTypeEnum.getEnumConstants().map { Pair(it.name(),it) }.toMap()
    internal val relationTypes: Map<String,R> = relationTypeEnum.getEnumConstants().map { Pair(it.name(),it) }.toMap()

    protected val graph: NFGraph = run {
        DataInputStream(input).use { dataStream ->
            // Header:
            checkHeaderValue(dataStream, GRAPH_MARKERS_HEADER)
            val version = dataStream.readInt() // TODO: if we need to check it for differences later

            // Partial Schema:
            checkHeaderValue(dataStream, GRAPH_MARKERS_SCHEMA_HEADER)
            val tempRelations = hashMapOf<Pair<String, String>, String>()
            val tempNodeTypes = hashSetOf<String>()

            val numberOfNodeTypes = dataStream.readInt()
            for (i in 1..numberOfNodeTypes) {
                val nodeTypeName = dataStream.readUTF()
            }

            val numberOfGroups = dataStream.readInt()
            for (i in 1..numberOfGroups) {
                val fromNodeType = dataStream.readUTF()
                val relationType = dataStream.readUTF()
                val numberOfTargets = dataStream.readInt()
                val pairKey = RelationshipPairKey(nodeTypes.get(fromNodeType)!!, relationTypes.get(relationType)!!)     // TODO: could fail, better error message?
                val group = relationshipGroups.getOrPut(pairKey, { hashSetOf() })
                for (x in 1..numberOfTargets) {
                    val toNodeType = dataStream.readUTF()
                    val trippleKey = RelationshipTrippleKey(pairKey.fromNode, pairKey.relationship, nodeTypes.get(toNodeType)!!)
                    group.add(trippleKey)
                }
            }

            val numberOfMirrors = dataStream.readInt()
            for (i in 1..numberOfMirrors) {
                val forward_fromNodeType = dataStream.readUTF()
                val forward_relationType = dataStream.readUTF()
                val forward_toNodeType = dataStream.readUTF()
                val backward_fromNodeType = dataStream.readUTF()
                val backward_relationType = dataStream.readUTF()
                val backward_toNodeType = dataStream.readUTF()
                val forwardTrippleKey = RelationshipTrippleKey(nodeTypes.get(forward_fromNodeType)!!, relationTypes.get(forward_relationType)!!, nodeTypes.get(forward_toNodeType)!!)
                val backwardTrippleKey = RelationshipTrippleKey(nodeTypes.get(backward_fromNodeType)!!, relationTypes.get(backward_relationType)!!, nodeTypes.get(backward_toNodeType)!!)
                relationshipMirrors.put(forwardTrippleKey, backwardTrippleKey)
            }

            // Ordinals:
            checkHeaderValue(dataStream, GRAPH_MARKERS_ORDINAL_HEADER)
            val numberOfOrdinalMaps = dataStream.readInt()
            for (i in 1..numberOfOrdinalMaps) {
                val nodeTypeName = dataStream.readUTF()
                val nodeOrdinalCount = dataStream.readInt()
                val nodeOrdMap = nodeOrdMap(nodeTypes.get(nodeTypeName)!!)
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



    public class OrdinalIterable(private val iterator: OrdinalIterator) : Iterable<Int> {
        override fun iterator(): Iterator<Int> {
            return OrdinalIteration(iterator)
        }
    }

    public class OrdinalIteration(private val iterator: OrdinalIterator) : AbstractIterator<Int>() {
        override fun computeNext() {
            val nextVal = iterator.nextOrdinal()
            if (nextVal != OrdinalIterator.NO_MORE_ORDINALS) {
                this.setNext(nextVal)
            } else {
                this.done()
            }
        }
    }

    // get connections from node A to all others
    // get connections from node A of relationship R
    // get connections from node A to B
    // get connectinos from node A to B of relationship R
    // does A have a connection type R
    // does A have a connection type R to B?
    // does A connect to B in any way

    // TODO: all for connection model again :-(

    protected fun getRelationTargets(nodeType: N, relation: R): List<N> {
        return relationshipGroups.getOrElse(RelationshipPairKey(nodeType,relation), { setOf<RelationshipTrippleKey<N,R>>() }).map { it.toNode }
    }

    protected fun makeRelationshipName(relation: R, toNodeType: N): String {
       return "${relation.name()}.${toNodeType.name()}"
    }

    protected fun NFGraph.getConnection(nodeAndOrd: NodeAndOrd<N>, relation: R): NodeAndId<N>? {
        return getConnection("", nodeAndOrd, relation)
    }

    protected fun NFGraph.getConnection(model: String, nodeAndOrd: NodeAndOrd<N>, relation: R): NodeAndId<N>? {
        val targets = getRelationTargets(nodeAndOrd.nodeType, relation)
        val qualifyFully = targets.size() > 1
        val results = linkedListOf<NodeAndId<N>>()
        targets.forEach { toNodeType ->
            val ordsForTarget = nodeOrdMap(toNodeType)
            val relate = if (qualifyFully) makeRelationshipName(relation, toNodeType) else relation.name()
            val ord = if (model.isNotEmpty()) {
                getConnection(nodeAndOrd.nodeType.name(), nodeAndOrd.ord, relate)
            } else {
                getConnection(model, nodeAndOrd.nodeType.name(), nodeAndOrd.ord, relate)
            }
            if (ord >= 0) {
                results.add(NodeAndId(toNodeType, ordsForTarget.get(ord)))
            }
        }
        if (results.size() > 1) {
            throw RuntimeException("Tried to get connections from graph node as single, but multiple existed instead: from ${nodeAndOrd} via ${relation} resulted in ${results.joinToString(",")}")
        }
        return results.firstOrNull()
    }

    protected fun NFGraph.getConnectionSet(nodeAndOrd: NodeAndOrd<N>, relation: R): Set<NodeAndId<N>> {
        return getConnectionSet("", nodeAndOrd, relation)
    }

    protected fun NFGraph.getConnectionSet(model: String, nodeAndOrd: NodeAndOrd<N>, relation: R): Set<NodeAndId<N>> {
        val targets = getRelationTargets(nodeAndOrd.nodeType, relation)
        val qualifyFully = targets.size() > 1
        val results = linkedListOf<NodeAndId<N>>()
        targets.forEach { toNodeType ->
            val ordsForTarget = nodeOrdMap(toNodeType)
            val relate = if (qualifyFully) makeRelationshipName(relation, toNodeType) else relation.name()
            val ordIterable = if (model.isEmpty()) {
                connectionsAsIterable(nodeAndOrd, relate)
            } else {
                connectionsAsIterable(model, nodeAndOrd, relate)
            }
            ordIterable.forEach { ord ->
                results.add(NodeAndId(toNodeType, ordsForTarget.get(ord)))
            }
        }
        return results.toSet()
    }


    public fun NodeAndOrd<N>.oneConnection(relation: R): NodeAndId<N>? {
        return graph.getConnection(this, relation)
    }

    public fun NodeAndOrd<N>.oneConnection(model: String, relation: R): NodeAndId<N>? {
        return graph.getConnection(model, this, relation)
    }

    public fun NodeAndOrd<N>.manyConnections(relation: R): Set<NodeAndId<N>> {
        return graph.getConnectionSet(this, relation)
    }

    public fun NodeAndOrd<N>.manyConnections(model: String, relation: R): Set<NodeAndId<N>>  {
        return graph.getConnectionSet(model, this, relation)
    }

   // TODO: for this as well public fun NodeAndOrdSet<N>.

    // TODO: for this as well public fun NodeAndOrdList<N>.

    protected fun NFGraph.connectionsAsIterable(nodeAndOrd: NodeAndOrd<N>, relation: R): OrdinalIterable = OrdinalIterable(this.getConnectionIterator(nodeAndOrd.nodeType.name(), nodeAndOrd.ord, relation.name()))
    protected fun NFGraph.connectionsAsIterable(model: String, nodeAndOrd: NodeAndOrd<N>, relation: R): OrdinalIterable = OrdinalIterable(this.getConnectionIterator(model, nodeAndOrd.nodeType.name(), nodeAndOrd.ord, relation.name()))
    protected fun NFGraph.connectionsAsIterable(nodeAndOrd: NodeAndOrd<N>, relationName: String): OrdinalIterable = OrdinalIterable(this.getConnectionIterator(nodeAndOrd.nodeType.name(), nodeAndOrd.ord, relationName))
    protected fun NFGraph.connectionsAsIterable(model: String, nodeAndOrd: NodeAndOrd<N>, relationName: String): OrdinalIterable = OrdinalIterable(this.getConnectionIterator(model, nodeAndOrd.nodeType.name(), nodeAndOrd.ord, relationName))


    protected fun NFGraph.connectionsAsIterator(nodeAndOrd: NodeAndOrd<N>, relation: R): OrdinalIteration = OrdinalIteration(this.getConnectionIterator(nodeAndOrd.nodeType.name(), nodeAndOrd.ord, relation.name()))
    protected fun NFGraph.connectionsAsIterator(model: String, nodeAndOrd: NodeAndOrd<N>, relation: R): OrdinalIteration = OrdinalIteration(this.getConnectionIterator(model, nodeAndOrd.nodeType.name(), nodeAndOrd.ord, relation.name()))

    protected fun NFGraph.connectionsAsSet(nodeAndOrd: NodeAndOrd<N>, relation: R): Set<Int> = OrdinalIterable(this.getConnectionIterator(nodeAndOrd.nodeType.name(), nodeAndOrd.ord, relation.name())).toSet()
    protected fun NFGraph.connectionsAsSet(model: String, nodeAndOrd: NodeAndOrd<N>, relation: R): Set<Int> = OrdinalIterable(this.getConnectionIterator(model, nodeAndOrd.nodeType.name(), nodeAndOrd.ord, relation.name())).toSet()

}

public inline fun <reified N : Enum<N>, reified R : Enum<R>> defineGraphSchema(defaultStructure: RelationStructure, init: GraphSchemaBuilder<N, R>.() -> Unit): CompiledGraphSchema<N, R> {
    val schema = GraphSchemaBuilder(javaClass<N>(), javaClass<R>(), defaultStructure)
    schema.init()
    return CompiledGraphSchema(schema)
}

public fun <N : Enum<N>, R : Enum<R>> constructGraph(schema: CompiledGraphSchema<N, R>, init: GraphBuilder<N,R>.() -> Unit): GraphBuilder<N,R> {
    val builder = GraphBuilder(schema)
    builder.init()
    return builder
}

public inline fun <reified N : Enum<N>, reified R : Enum<R>> useGraph(inputStream: InputStream, run: ReadOnlyGraph<N, R>.() -> Unit): ReadOnlyGraph<N, R> {
    val graph = ReadOnlyGraph<N,R>(javaClass<N>(), javaClass<R>(), inputStream)
    graph.run()
    return graph
}


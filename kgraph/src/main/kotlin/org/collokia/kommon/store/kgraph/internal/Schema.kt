package org.collokia.kommon.store.kgraph.internal

import kotlin.properties.Delegates

private enum class RelationCardinality(override val flags: Int) : GraphRelationOptions {
    SINGLE(com.netflix.nfgraph.spec.NFPropertySpec.SINGLE),
    MULTIPLE(com.netflix.nfgraph.spec.NFPropertySpec.MULTIPLE)
}

private enum class RelationScope(override val flags: Int) : GraphRelationOptions {
    GLOBAL(com.netflix.nfgraph.spec.NFPropertySpec.GLOBAL),
    MODEL(com.netflix.nfgraph.spec.NFPropertySpec.MODEL_SPECIFIC)
}

enum class RelationStructure(override val flags: Int) : GraphRelationOptions {
    COMPACT(com.netflix.nfgraph.spec.NFPropertySpec.COMPACT),
    HASH(com.netflix.nfgraph.spec.NFPropertySpec.HASH)
}

private interface GraphRelationOptions {
    val flags: Int
}

private fun GraphRelationOptions.plus(other: GraphRelationOptions): GraphRelationOptions = TempGraphFlags(this.flags or other.flags)
private fun GraphRelationOptions.minus(other: GraphRelationOptions): GraphRelationOptions = TempGraphFlags(this.flags and other.flags.inv())
private class TempGraphFlags(override val flags: Int) : GraphRelationOptions

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

    public fun connectOneEdge(relation: R): GraphRelationPredicateEdge<N, R> {
        forwardRelation = relation
        forwardFlags = forwardFlags - RelationCardinality.MULTIPLE + RelationCardinality.SINGLE
        return GraphRelationPredicateEdge(this)
    }

    public fun connectEdges(relation: R): GraphRelationPredicateEdge<N, R> {
        forwardRelation = relation
        forwardFlags = this@GraphRelationBuilder.forwardFlags - RelationCardinality.SINGLE + RelationCardinality.MULTIPLE
        return GraphRelationPredicateEdge(this)
    }

    internal fun completeEnough() {
        relations.add(this)
    }
}

internal class GraphRelationPredicateEdge<N : Enum<N>, R : Enum<R>>(private val builder: GraphRelationBuilder<N, R>) {
    public fun to(nodeType: N): GraphRelationPredicateNoBackwards<N, R> {
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

    public fun autoMirrorOneEdge(backRelation: R): GraphRelationPredicateWithBackEdge<N, R> {
        builder.backwardRelation = backRelation
        builder.backwardFlags = builder.backwardFlags - RelationCardinality.MULTIPLE + RelationCardinality.SINGLE
        return GraphRelationPredicateWithBackEdge(builder)
    }

    public fun autoMirrorEdges(backRelation: R): GraphRelationPredicateWithBackEdge<N, R> {
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
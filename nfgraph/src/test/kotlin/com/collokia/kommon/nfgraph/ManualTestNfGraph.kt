package com.collokia.kommon.nfgraph

import org.junit.Test
import com.netflix.nfgraph.OrdinalIterator
import com.netflix.nfgraph.compressed.NFCompressedGraph
import kotlin.support.AbstractIterator
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import com.collokia.kommon.testing
import com.collokia.kommon.nfgraph.MyNodes.*
import com.collokia.kommon.nfgraph.MyRelations.*
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.test.fail


enum class MyNodes : GraphNodeType {
    SOPost
    SOTag
    MavenGA
    WebPage
}

enum class MyRelations: GraphRelationType {
    TaggedWith
    TaggedSOPost
    TaggedWebPage
    TaggedMavenGA
    TagWiki
    OwnedByTag
}

public class TestNfGraph {

    [Test] fun basicGraphBuilding() {
        val schema = defineGraphSchema(BidirectionRelation.MANY_TO_MANY, RelationStructure.COMPACT) {
            nodesFrom(MyNodes.values())

            from(SOTag).edge(TaggedSOPost).toMany(SOPost).manyEdgesBack(TaggedWith)
            from(SOTag).edge(TaggedWebPage).toMany(WebPage).manyEdgesBack(TaggedWith)
            from(SOTag).edge(TaggedMavenGA).toMany(MavenGA).manyEdgesBack(TaggedWith)
            from(SOTag).edge(TagWiki).toOne(SOPost).oneEdgeBack(OwnedByTag)
        }

        val builder = constructGraph(schema) {
            connect(SOPost["1"], TaggedWith, SOTag["java"])
            connect(SOPost["2"], TaggedWith, SOTag["scala"])

            connect(SOPost["3"], TaggedWith, SOTag["java"])
            connect(SOPost["3"], TaggedWith, SOTag["scala"])

            connect(SOTag["java"], TagWiki, SOPost["99"])
            try {
                connect(SOTag["java"], TagWiki, SOPost["100"])
                fail("Should not be able to add a second tag when cardinality is 1")
            }
            catch (ex: Throwable) {
                // continue
            }
        }

        val outputBuffer = ByteArrayOutputStream()
        builder.serialize(outputBuffer)
        val graph = Graph(ByteArrayInputStream(outputBuffer.toByteArray()))

        with (graph) {
            // Tags has documents (variations of each way to check)
            assertTrue(connectionsContainsOnly(SOTag["java"], TaggedSOPost, setOf("1", "3")))
            assertTrue(connectionsContainsOnly(SOTag["java"], TaggedSOPost, SOPost.withKeys("1", "3")))

            assertTrue(connectionsContainsOnly(SOTag["scala"], TaggedSOPost, setOf("2", "3")))
            assertTrue(connectionsContainsOnly(SOTag["scala"], TaggedSOPost, SOPost.withKeys("2", "3")))

            // Document has tags (variations of each way to check)
            assertTrue(connectionsContainsOnly(SOPost["1"], TaggedWith, setOf("java")))
            assertTrue(connectionsContainsOnly(SOPost["1"], TaggedWith, SOTag.withKeys("java")))
            assertTrue(connectionsContainsOnly(SOPost["2"], TaggedWith, setOf("scala")))
            assertTrue(connectionsContainsOnly(SOPost["2"], TaggedWith, SOTag.withKeys("scala")))
            assertTrue(connectionsContainsOnly(SOPost["3"], TaggedWith, setOf("java", "scala")))
            assertTrue(connectionsContainsOnly(SOPost["3"], TaggedWith, SOTag.withKeys("java", "scala")))
        }
    }


}

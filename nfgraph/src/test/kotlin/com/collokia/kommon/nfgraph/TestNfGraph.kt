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


enum class MyNodes {
    Movie
    Actor
    Director
    Award

}

enum class MyRelations {
    StarredIn
    Starring
    DirectedBy
    Directed
    WonAward
    AwardWinner
}

public class TestNfGraph {

    [Test] fun basicGraphBuilding() {
        val schema = defineGraphSchema<MyNodes, MyRelations>(RelationStructure.COMPACT) {
            from(Movie).manyEdges(Starring).target(Actor).mirrorManyEdges(StarredIn)
            from(Movie).oneEdge(DirectedBy).target(Director).mirrorManyEdges(Directed)
            from(Movie).manyEdges(WonAward).target(Award).mirrorManyEdges(AwardWinner)
            from(Actor).manyEdges(WonAward).target(Award).mirrorManyEdges(AwardWinner)  // creates award with two identical relations but to different targets
            from(Director).manyEdges(WonAward).target(Award).mirrorManyEdges(AwardWinner)

            // TODO: check dupes overwriting with same settings

            modelScope() {
                // TODO: test relationships in model scope
            }
        }

        val builder = constructGraph(schema) {
            connect(Movie["Star Wars"], Starring, Actor["Harrison Ford"])
            connect(Movie["Star Wars"], Starring, Actor["Mark Hamill"])
            connect(Movie["Star Wars"], Starring, Actor["Carrie Fisher"])
            connect(Movie["Star Wars"], Starring, Actor["Peter Mayhew"])
            connect(Movie["Star Wars"], DirectedBy, Director["George Lucas"])
            connect(Movie["Star Wars"], WonAward, Award["1979 Academy Award for Best Visual Effects"])
            connect(Movie["Star Wars"], WonAward, Award["1979 Academy Award for Best Original Music Score"])
            connect(Actor["Harrison Ford"], WonAward, Award["2000 People's Choice Award"])
            connect(Director["George Lucas"], WonAward, Award["2005 AFI Life Achievement Award"])
            connect(Award["1994 Academy Award for Best Picture"], AwardWinner, Director["Steven Spielberg"])
            connect(Movie["Indiana Jones"], Starring, Actor["Harrison Ford"])
            connect(Movie["Indiana Jones"], DirectedBy, Director["Steven Spielberg"])

            connect(Actor["Harrison Ford"], WonAward, Award["People's Choice Award"])
            connect(Director["Steven Spielberg"], WonAward, Award["People's Choice Award"])
            connect(Movie["Star Wars"], WonAward, Award["People's Choice Award"])
            connect(Movie["Indiana Jones"], WonAward, Award["People's Choice Award"])
        }

        fun Set<NodeAndId<MyNodes>>.hasOnly(testFor: Set<NodeAndId<MyNodes>>): Boolean {
            return (this.size() == testFor.size() && this.containsAll(testFor))
        }

        val outputBuffer = ByteArrayOutputStream()
        builder.serialize(outputBuffer)

        useGraph<MyNodes, MyRelations>(ByteArrayInputStream(outputBuffer.toByteArray())) {
            assertTrue(Actor["Harrison Ford"].manyConnections(StarredIn).hasOnly(setOf(NodeAndId(Movie, "Star Wars"), NodeAndId(Movie, "Indiana Jones"))))
            assertEquals(NodeAndId(Movie, "Star Wars"), Actor["Mark Hamill"].oneConnection(StarredIn))
            assertEquals(NodeAndId(Movie, "Star Wars"), Actor["Carrie Fisher"].oneConnection(StarredIn))
            assertEquals(NodeAndId(Movie, "Star Wars"), Actor["Peter Mayhew"].oneConnection(StarredIn))
            assertTrue(Movie["Star Wars"].manyConnections(Starring).hasOnly(setOf(NodeAndId(Actor, "Harrison Ford"),
                    NodeAndId(Actor, "Mark Hamill"),
                    NodeAndId(Actor, "Carrie Fisher"),
                    NodeAndId(Actor, "Peter Mayhew"))))
            assertEquals(NodeAndId(Director, "George Lucas"), Movie["Star Wars"].oneConnection(DirectedBy))
            assertEquals(NodeAndId(Movie, "Star Wars"), Director["George Lucas"].oneConnection(Directed))

            assertEquals(NodeAndId(Director, "Steven Spielberg"), Award["1994 Academy Award for Best Picture"].oneConnection(AwardWinner))
            assertEquals(NodeAndId(Award, "2005 AFI Life Achievement Award"), Director["George Lucas"].oneConnection(WonAward))
            assertEquals(NodeAndId(Movie, "Star Wars"), Award["1979 Academy Award for Best Visual Effects"].oneConnection(AwardWinner))
            assertEquals(NodeAndId(Movie, "Star Wars"), Award["1979 Academy Award for Best Original Music Score"].oneConnection(AwardWinner))

            val actualSet = Award["People's Choice Award"].manyConnections(AwardWinner)
            val expectedSet= setOf(NodeAndId(Movie, "Star Wars"), NodeAndId(Movie, "Indiana Jones"),
                    NodeAndId(Director, "Steven Spielberg"), NodeAndId(Actor, "Harrison Ford"))
            assertTrue(actualSet.hasOnly(expectedSet))
        }

    }
}

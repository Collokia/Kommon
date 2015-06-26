package org.collokia.kommon.web

import org.junit.Test
import kotlin.reflect.KMemberProperty
import kotlin.reflect.jvm.kotlin
import kotlin.test.assertEquals
import kotlin.test.fail


class TestKt7721 {
    fun dispatchFailure(target: KMemberProperty<out Any, *>) {
        val first = ::member1
        val second = ::member1
        assertEquals(first, second) // works fine

        val member = TestKt7721::member1
        if (target == member) {

        }
        else {
            fail("should work, but fails")  // fails
        }
    }

    val member1 = fun (abc: String): String = abc

    @Test fun testKt7721_failure() {
        javaClass<TestKt7721>().kotlin.properties.filter { it.name == "member1" }.forEach { prop ->
            dispatchFailure(prop)
        }
    }

    @Test fun testKt7721_success() {
        TestKt7721::class.properties.filter { it.name == "member1" }.forEach { prop ->
            dispatchFailure(prop)
        }
    }
}
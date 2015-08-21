package org.collokia.kommon

public fun <T> T.then(initWith: (T) -> Unit): T {
    initWith(this)
    return this
}

public fun testing(scope: ()->Unit): Unit {
    scope()
}
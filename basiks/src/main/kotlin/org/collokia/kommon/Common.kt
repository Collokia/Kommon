package org.collokia.kommon

public fun <T> T.verifiedBy(verifyWith: (T) -> Unit): T {
    verifyWith(this)
    return this
}

public fun <T> T.initializedBy(initWith: (T) -> Unit): T {
    initWith(this)
    return this
}

public fun <T> T.then(initWith: (T) -> Unit): T {
    initWith(this)
    return this
}

public fun testing(scope: ()->Unit): Unit {
    scope()
}
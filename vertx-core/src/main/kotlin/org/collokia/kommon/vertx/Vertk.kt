package org.collokia.kommon.vertx

import io.vertx.core.*
import io.vertx.core.Context
import nl.komponents.kovenant.*
import nl.komponents.kovenant.Dispatcher as KovenantDispatcher
import nl.komponents.kovenant.Context as KovenantContext
import kotlin.platform.platformName
import kotlin.reflect.KClass
import kotlin.reflect.jvm.java


object VertxInit {
    init {
        Kovenant.context = VertxKovenantContext()
    }

    public inline fun ensure() {
        // TODO: here to be sure we have intiailized anything related before using,
        //       although this function may remain empty it causes initializers on the
        //       object to run.
    }
}

public fun vertx(): Promise<Vertx, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<Vertx, Throwable>()
    try {
        deferred.resolve(Vertx.vertx())
    }
    catch (ex: Throwable) {
        deferred.reject(ex)
    }
    return deferred.promise
}

public fun vertx(options: VertxOptions): Promise<Vertx, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<Vertx, Throwable>()
    try {
        deferred.resolve(Vertx.vertx(options))
    }
    catch (ex: Throwable) {
        deferred.reject(ex)
    }
    return deferred.promise
}

public fun vertxCluster(options: VertxOptions): Promise<Vertx, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<Vertx, Throwable>()
    Vertx.clusteredVertx(options, promiseResult(deferred))
    return deferred.promise
}
public fun vertxContext(): Context = Vertx.currentContext()

public fun Vertx.promiseDeployVerticle(verticle: Verticle): Promise<String, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<String, Throwable>()
    deployVerticle(verticle, promiseResult(deferred))
    return deferred.promise
}

public fun Vertx.promiseDeployVerticle(verticle: Verticle, options: DeploymentOptions): Promise<String, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<String, Throwable>()
    deployVerticle(verticle, options, promiseResult(deferred))
    return deferred.promise
}

public fun  <T : AbstractVerticle> Vertx.promiseDeployVerticle(verticleClass: KClass<T>): Promise<String, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<String, Throwable>()
    deployVerticle(verticleClass.java.getName(), promiseResult(deferred))
    return deferred.promise
}

public fun  <T : AbstractVerticle> Vertx.promiseDeployVerticle(verticleClass: KClass<T>, options: DeploymentOptions): Promise<String, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<String, Throwable>()
    deployVerticle(verticleClass.java.getName(), options, promiseResult(deferred))
    return deferred.promise
}

public fun  <T : AbstractVerticle> Vertx.promiseDeployVerticle(verticleClass: Class<T>): Promise<String, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<String, Throwable>()
    deployVerticle(verticleClass.getName(), promiseResult(deferred))
    return deferred.promise
}

public fun  <T : AbstractVerticle> Vertx.promiseDeployVerticle(verticleClass: Class<T>, options: DeploymentOptions): Promise<String, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<String, Throwable>()
    deployVerticle(verticleClass.getName(), options, promiseResult(deferred))
    return deferred.promise
}

public fun Vertx.promiseDeployVerticle(name: String): Promise<String, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<String, Throwable>()
    deployVerticle(name, promiseResult(deferred))
    return deferred.promise
}

public fun Vertx.promiseDeployVerticle(name: String, options: DeploymentOptions): Promise<String, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<String, Throwable>()
    deployVerticle(name, options, promiseResult(deferred))
    return deferred.promise
}

public fun <T : AbstractVerticle> Vertx.deployVerticle(verticleClass: KClass<T>): Unit {
    deployVerticle(verticleClass.java.getName())
}

public fun <T : AbstractVerticle> Vertx.deployVerticle(verticleClass: Class<T>): Unit {
    deployVerticle(verticleClass.getName())
}

public fun Vertx.promiseUndeploy(deploymentId: String): Promise<Void, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<Void, Throwable>()
    this.undeploy(deploymentId, promiseResult(deferred))
    return deferred.promise
}

public fun Vertx.promiseClose(): Promise<Void, Throwable> {
    val deferred = deferred<Void, Throwable>()
    this.close(promiseResult(deferred))
    return deferred.promise
}

public fun <T> Vertx.promiseExecuteBlocking(blockingCode: ()->T): Promise<T, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<T, Throwable>()
    this.executeBlocking({ response ->
        try {
            response.complete(blockingCode())
        }
        catch (ex: Throwable) {
            response.fail(ex)
        }
    }, promiseResult(deferred))
    return deferred.promise
}

public fun <T> Vertx.executeBlocking(blockingCode: ()->T): Promise<T, Throwable> {
    VertxInit.ensure()

    val deferred = deferred<T, Throwable>()
    this.executeBlocking({ response ->
        try {
            response.complete(blockingCode())
        }
        catch (ex: Throwable) {
            response.fail(ex)
        }
    }, promiseResult(deferred))
    return deferred.promise
}

fun <T> promiseResult(deferred: Deferred<T, Throwable>): (AsyncResult<T>) -> Unit {
    return { completion ->
        if (completion.succeeded()) {
            deferred.resolve(completion.result())
        } else {
            deferred.reject(completion.cause())
        }
    }
}

// -- part of Kovenant in the near future, connects the context of Kovenant promises to dispatch into Vert.x thread and context management.

class VertxKovenantContext() : nl.komponents.kovenant.Context {
    override val callbackContext: DispatcherContext
        get() = VertxCallbackDispatcherContext(vertxContext())
    override val workerContext: DispatcherContext
        get() = VertxWorkerDispatcherContext(vertxContext())

    override val multipleCompletion: (Any, Any) -> Unit
        get() = { curVal: Any, newVal: Any -> throw IllegalStateException("Value[$curVal] is set, can't override with new value[$newVal]") }
}

class VertxCallbackDispatcherContext(private val ctx: Context) : DispatcherContext {
    override val dispatcher: Dispatcher
        get() = VertxCallbackDispatcher(ctx)
    override val errorHandler: (Exception) -> Unit
        get() = throw UnsupportedOperationException()

}

class VertxCallbackDispatcher(private val ctx: Context) : BasicDispatcher() {
    override fun offer(task: () -> Unit): Boolean {
        ctx.runOnContext {
            task()
        }
        return true
    }

}

class VertxWorkerDispatcherContext(private val ctx: Context) : DispatcherContext {
    override val dispatcher: Dispatcher
        get() = VertxWorkerDispatcher(ctx)
    override val errorHandler: (Exception) -> Unit
        get() = throw UnsupportedOperationException()

}

class VertxWorkerDispatcher(private val ctx: Context) : BasicDispatcher() {
    override fun offer(task: () -> Unit): Boolean {
        ctx.owner().executeBlocking<Unit>( {
            task()
        }, null)
        return true
    }
}

abstract class BasicDispatcher : Dispatcher {
    override fun stop(force: Boolean, timeOutMs: Long, block: Boolean): List<() -> Unit> = throw UnsupportedOperationException()
    override fun tryCancel(task: () -> Unit): Boolean = false
    override val terminated: Boolean
        get() = throw UnsupportedOperationException()
    override val stopped: Boolean
        get() = throw UnsupportedOperationException()
}

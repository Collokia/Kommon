package org.collokia.kommon.vertk

import io.vertx.core.*
import io.vertx.core.Context
import nl.mplatvoet.komponents.kovenant.*
import nl.mplatvoet.komponents.kovenant.Dispatcher as KovenantDispatcher
import nl.mplatvoet.komponents.kovenant.Context as KovenantContext
import kotlin.platform.platformName
import kotlin.reflect.KClass
import kotlin.reflect.jvm.java

public fun vertx(): Promise<Vertx, Throwable> {
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
    val deferred = deferred<Vertx, Throwable>()
    Vertx.clusteredVertx(options, promiseResult(deferred))
    return deferred.promise
}
public fun vertxContext(): Context = Vertx.currentContext()

public fun Vertx.promiseDeployVerticle(verticle: Verticle): Promise<String, Throwable> {
    val deferred = deferred<String, Throwable>()
    deployVerticle(verticle, promiseResult(deferred))
    return deferred.promise
}

public fun Vertx.promiseDeployVerticle(verticle: Verticle, options: DeploymentOptions): Promise<String, Throwable> {
    val deferred = deferred<String, Throwable>()
    deployVerticle(verticle, options, promiseResult(deferred))
    return deferred.promise
}

public fun  <T : AbstractVerticle> Vertx.promiseDeployVerticle(verticleClass: KClass<T>): Promise<String, Throwable> {
    val deferred = deferred<String, Throwable>()
    deployVerticle(verticleClass.java.getName(), promiseResult(deferred))
    return deferred.promise
}

public fun  <T : AbstractVerticle> Vertx.promiseDeployVerticle(verticleClass: KClass<T>, options: DeploymentOptions): Promise<String, Throwable> {
    val deferred = deferred<String, Throwable>()
    deployVerticle(verticleClass.java.getName(), options, promiseResult(deferred))
    return deferred.promise
}

public fun  <T : AbstractVerticle> Vertx.promiseDeployVerticle(verticleClass: Class<T>): Promise<String, Throwable> {
    val deferred = deferred<String, Throwable>()
    deployVerticle(verticleClass.getName(), promiseResult(deferred))
    return deferred.promise
}

public fun  <T : AbstractVerticle> Vertx.promiseDeployVerticle(verticleClass: Class<T>, options: DeploymentOptions): Promise<String, Throwable> {
    val deferred = deferred<String, Throwable>()
    deployVerticle(verticleClass.getName(), options, promiseResult(deferred))
    return deferred.promise
}

public fun Vertx.promiseDeployVerticle(name: String): Promise<String, Throwable> {
    val deferred = deferred<String, Throwable>()
    deployVerticle(name, promiseResult(deferred))
    return deferred.promise
}

public fun Vertx.promiseDeployVerticle(name: String, options: DeploymentOptions): Promise<String, Throwable> {
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

public fun <T> promiseResult(deferred: Deferred<T, Throwable>): (AsyncResult<T>) -> Unit {
    return { completion ->
        if (completion.succeeded()) {
            deferred.resolve(completion.result())
        } else {
            deferred.reject(completion.cause())
        }
    }
}

             /*
private fun KovenantContext.tryWork(fn: () -> Unit) = workerDispatcher.offer (fn, workerError)

private fun KovenantDispatcher.offer(fn: () -> Unit, errorFn: (Exception) -> Unit) {
    try {
        this.offer(fn)
    } catch (e: Exception) {
        errorFn(e)
    }
}

public fun <V, R> Promise<V, Throwable>.then(bind: (V) -> R): Promise<R, Throwable> {
    val context = when (this) {
        is ContextAware -> this.context
        else -> Kovenant.context
    }

    val deferred = deferred<R, Throwable>(context)
    success {
        context.tryWork {
            try {
                val result = bind(it)
                deferred.resolve(result)
            } catch(e: Exception) {
                deferred.reject(e)
            }
        }
    }
    fail {
        deferred.reject(it)
    }
    return deferred.promise
}

         */
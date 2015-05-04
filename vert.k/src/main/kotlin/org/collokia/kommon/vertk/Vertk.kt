package org.collokia.kommon.vertk

import io.vertx.core.*
import io.vertx.core.Context
import nl.mplatvoet.komponents.kovenant.*
import kotlin.platform.platformName
import kotlin.reflect.KClass
import kotlin.reflect.jvm.java

public fun vertx(): Vertx = Vertx.vertx()
public fun vertx(options: VertxOptions): Vertx = Vertx.vertx(options)
public fun vertxCluster(options: VertxOptions): Promise<Vertx, Throwable> {
    val deferred = deferred<Vertx, Throwable>()
    Vertx.clusteredVertx(options, promiseResult(deferred))
    return deferred.promise
}
public fun vertxContext(): Context = Vertx.currentContext()

public fun vertxKovenantDispatcher(): Dispatcher {
  return object : Dispatcher {
      override val stopped: Boolean
          get() = throw UnsupportedOperationException()
      override val terminated: Boolean
          get() = throw UnsupportedOperationException()

      override fun offer(task: () -> Unit): Boolean {
          throw UnsupportedOperationException()
      }

      override fun stop(force: Boolean, timeOutMs: Long, block: Boolean): List<() -> Unit> {
          throw UnsupportedOperationException()
      }

      override fun tryCancel(task: () -> Unit): Boolean {
          throw UnsupportedOperationException()
      }

  }
}

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



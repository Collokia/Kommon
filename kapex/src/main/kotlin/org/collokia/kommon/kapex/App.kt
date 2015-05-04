package org.collokia.kommon.kapex

import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerRequest
import io.vertx.ext.apex.Router
import nl.mplatvoet.komponents.kovenant.Kovenant
import nl.mplatvoet.komponents.kovenant.Promise
import nl.mplatvoet.komponents.kovenant.async
import nl.mplatvoet.komponents.kovenant.then
import kotlin.platform.platformStatic
import kotlin.reflect.jvm.java
import org.collokia.kommon.vertk.*


public class KapexVerticle : AbstractVerticle() {
    companion object {
        [platformStatic]
        public fun main(args: Array<String>) {

            val vertx = Vertx.vertx(VertxOptions().setWorkerPoolSize(Runtime.getRuntime().availableProcessors()*2))
            vertx.promiseDeployVerticle(KapexVerticle::class) success { deploymentId ->
               println("Deployed as $deploymentId}")
            } fail { failureException ->
               println("Failed due to $failureException")
            }

            async {
                println("do")
            }  then {

            }

        }
    }

    override fun start() {
         val router = Router.router(vertx)
         router.route().handler { context ->
             context.response().putHeader("content-type", "text/html").end("Hello World!")
         }
        vertx.createHttpServer().requestHandler { router.accept(it) }.listen(8080)
    }
}

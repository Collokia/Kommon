package org.collokia.kommon.kapex

import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.datatype.jsr310.JSR310Module
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.net.JksOptions
import io.vertx.ext.apex.Router
import io.vertx.ext.apex.RoutingContext
import io.vertx.ext.apex.handler.BodyHandler
import io.vertx.ext.apex.handler.CookieHandler
import io.vertx.ext.apex.handler.SessionHandler
import io.vertx.ext.apex.sstore.ClusteredSessionStore
import jet.runtime.typeinfo.JetValueParameter
import nl.mplatvoet.komponents.kovenant.async
import org.collokia.kommon.jdk.strings.mustEndWith
import org.collokia.kommon.jdk.strings.mustNotEndWith
import org.collokia.kommon.jdk.strings.mustNotStartWith
import org.collokia.kommon.jdk.strings.mustStartWith
import org.collokia.kommon.vertk.promiseDeployVerticle
import org.joda.time.DateTime
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import org.reflections.scanners.TypeAnnotationsScanner
import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target
import java.lang.reflect.Modifier
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.platform.platformStatic
import kotlin.reflect.KCallable
import kotlin.reflect.jvm.javaField
import kotlin.reflect.jvm.kotlin

Retention(RetentionPolicy.RUNTIME)
Target(ElementType.TYPE)
annotation class KapexController(val path: String)

Retention(RetentionPolicy.RUNTIME)
Target(ElementType.TYPE)
annotation class KapexRoute(val verb: HttpMethod, val path: String = "", val accepts: String = "", val produces: String = "")

trait InterceptRequests {
    fun interceptRequest(target: KCallable<Any>)
}

trait InterceptFailures {
    fun interceptFailures(target: KCallable<Any>)
}

trait ContextFactory<T> {
    fun createContext(from: RoutingContext): T
}

KapexController("/app/myform")
object TestRoute : InterceptRequests, InterceptFailures, ContextFactory<TestRoute.TestContext> {
    override fun interceptRequest(target: KCallable<Any>) {
        if (target == indexGet) {
            println("I know who this is!!")
        }
        println("Intercept")
    }

    override fun interceptFailures(target: KCallable<Any>) {
        if (target == indexGet) {
            println("I know who this is!!")
        }
        println("Intercept")
    }

    override fun createContext(from: RoutingContext): TestContext {
        return TestContext(from)
    }

    KapexRoute(HttpMethod.GET)
    val indexGet = fun TestContext.(xyz: Int, farf: Long): String {
        return "hello html"
    }

    KapexRoute(HttpMethod.POST, "edit")
    val editPost = fun TestContext.(xyz: Int, farf: Long) {
        // must always throw a redirect to show
        throw KapexRedirect("/app/myform?xyz=${xyz}&farf=${farf}")
    }

    KapexRoute(HttpMethod.GET, "edit")
    val editView = fun TestContext.(xyz: Int, farf: Long): String {
        return "hello html"
    }

    class TestContext(private val from: RoutingContext) {

    }

}

class KapexRedirect(val path: String) : Exception() {

}

object ReflectionCache {
    val objects = ConcurrentHashMap<Class<*>, Any>()
}

fun Class<*>.objectInstance(): Any? {
    return ReflectionCache.objects.getOrPut(this) {
        try {
            val field = getDeclaredField("INSTANCE\$")
            if (Modifier.isStatic(field.getModifiers()) && Modifier.isPublic(field.getModifiers())) {
                field[null]!!
            } else NullMask
        } catch (e: NoSuchFieldException) {
            NullMask
        }
    }.unmask()
}

private object NullMask

private fun Any.unmask(): Any? = if (this == NullMask) null else this

public class KapexVerticle : AbstractVerticle() {
    companion object {
        [platformStatic]
        public fun main(args: Array<String>) {
            val vertx = Vertx.vertx(VertxOptions().setWorkerPoolSize(Runtime.getRuntime().availableProcessors() * 2))
            vertx.promiseDeployVerticle(KapexVerticle::class) success { deploymentId ->
                println("Deployed as $deploymentId")
            } fail { failureException ->
                println("Failed due to $failureException")
            }
        }
    }

    object EmptyContextFactory : ContextFactory<RoutingContext> {
        override fun createContext(from: RoutingContext): RoutingContext {
            return from
        }
    }

    override fun start() {
        async {
            val JSON = jacksonObjectMapper()
                    .registerModule(JodaModule())
                    .registerModule(GuavaModule())
                    .registerModule(JSR310Module())
                    .registerModule(Jdk8Module())
            val router = Router.router(vertx)
            router.route().handler(CookieHandler.create())
            router.route().handler(SessionHandler.create(ClusteredSessionStore.create(vertx)).setSessionTimeout(TimeUnit.HOURS.toMillis(12)))

            val reflections = Reflections(this.javaClass.getPackage(), SubTypesScanner(false), TypeAnnotationsScanner())
            val controllerClasses = reflections.getTypesAnnotatedWith(javaClass<KapexController>()) as  Set<Class<Any>>

            controllerClasses.forEach { controller ->
                val instance = controller.objectInstance()
                if (instance != null) {
                    val controllerAnnotation = controller.getDeclaredAnnotation(javaClass<KapexController>())
                    controller.kotlin.properties.forEach { prop ->
                        if (prop.javaField != null) {
                            // TODO: check if this has a HTTP verb on it, if not ignore
                            val propJava = prop.javaField!!
                            val routeAnnotation = propJava.getAnnotation(javaClass<KapexRoute>())

                            if (routeAnnotation != null) {
                                val typeNameOfField = propJava.getType().getName()
                                if (typeNameOfField.startsWith("kotlin.ExtensionFunction")) {
                                    val memberFunction = prop.get(instance)
                                    if (memberFunction != null) {
                                        val methods = memberFunction.javaClass.getMethods()
                                        val invokeMethod = methods.filter { method -> method.getName() == "invoke" }.filter { it.getParameterAnnotations().all { it.any { it.annotationType() == javaClass<JetValueParameter>() } } }.firstOrNull()

                                        if (invokeMethod != null) {
                                            val paramAnnotations = invokeMethod.getParameterAnnotations()
                                            val receiverName = paramAnnotations[0].first { it.annotationType() == javaClass<JetValueParameter>() } as JetValueParameter
                                            if (receiverName.name() != "\$receiver") {
                                                throw RuntimeException("Invalid property ${prop.name} on class ${controller.getCanonicalName()} has HTTP verb but cannot find an invokable function in the extension method synthesized class, with first \$receiver parameter")
                                            }

                                            // TODO: validate if the extension method is on a Context class that has a context factory, or is the default type of RoutingContext?
                                            // .returnType
                                            // .parameterTypes [0] should be instance of class being extended
                                            //                 [1..N] params

                                            val receiverType = invokeMethod.getParameterTypes().first()

                                            val contextFactory: ContextFactory<*> = if (instance is ContextFactory<*>) instance else EmptyContextFactory

                                            // TODO check the context factory method return type if from the class?
                                            val contextType = if (instance is ContextFactory<*>) {
                                                val createMethod = controller.getMethod("createContext", javaClass<RoutingContext>())
                                                createMethod.getReturnType()
                                            } else {
                                                javaClass<RoutingContext>()
                                            }

                                            if (!receiverType.isAssignableFrom(contextType)) {
                                                throw RuntimeException("Invalid property ${prop.name} on class ${controller.getCanonicalName()} has HTTP verb but the function extends ${receiverType.getName()} instead of the expected context type ${contextType.getName()}")
                                            }

                                            val returnType = invokeMethod.getReturnType()
                                            val paramTypes = invokeMethod.getParameterTypes().drop(1)
                                            val paramNames = paramAnnotations.drop(1).map { it.filterIsInstance(javaClass<JetValueParameter>()).first().name() }

                                            val simpleTypes = listOf(javaClass<Boolean>(), javaClass<Number>(), javaClass<String>(), javaClass<DateTime>(), javaClass<Date>())

                                            [data] class ParamDef(val name: String, val type: Class<*>, val deser: (String?) -> Any?) {
                                                val isComplexType = !simpleTypes.any { type.isAssignableFrom(it) }
                                            }

                                            val paramDefs = paramNames.zip(paramTypes).map { ParamDef(it.first, it.second, { it }) }
                                            val paramCount = paramDefs.size()
                                            val paramContainsComplex = paramDefs.any { it.isComplexType }

                                            val basePath = controllerAnnotation.path.mustStartWith('/').mustEndWith('/')
                                            val suffixPath = routeAnnotation.path.mustNotStartWith('/').mustNotEndWith('/')
                                            val fullPath = (basePath + suffixPath).mustNotEndWith('/')

                                            if (paramContainsComplex || (routeAnnotation.verb != HttpMethod.GET && routeAnnotation.verb != HttpMethod.HEAD)) {
                                                // might get complex values from the body, so preload body in these cases for the same route
                                                // don't worry about GET vs. PUT and which have bodies, this is already optimized in the BodyHandlerImpl
                                                router.route(fullPath).method(routeAnnotation.verb).handler(BodyHandler.create().setBodyLimit(4 * 1024))  // TODO: allow this to be configured for max body size
                                            }

                                            val route = router.route(fullPath).method(routeAnnotation.verb)
                                            if (routeAnnotation.accepts.isNotEmpty()) {
                                                route.consumes(routeAnnotation.accepts)
                                            }
                                            if (routeAnnotation.produces.isNotEmpty()) {
                                                route.produces(routeAnnotation.produces)
                                            }

                                            route.handler { routeContext ->
                                                val requestContext = contextFactory.createContext(routeContext)

                                                // need to bind each parameter, for each parameter
                                                // if simple types, from path, then from query/form
                                                // if not simple type, from body as JSON or multi-part form, or its fields from "name.valueName" type patterns

                                                val request = routeContext.request()
                                                val useValues = linkedListOf<Any?>()
                                                paramDefs.forEach { param ->
                                                    val paramValue: Any? = if (param.isComplexType) {
                                                        if (request.isExpectMultipart()) {
                                                            val parmPrefix = param.name + "."
                                                            val tempMap = request.params().entries().filter { it.getKey().startsWith(parmPrefix) }.map { it.getKey().mustNotStartWith(parmPrefix) to it.getValue() }.toMap()
                                                            if (tempMap.isEmpty()) {
                                                                throw RuntimeException("cannot bind parameter ${param.name} from incoming form, require variables named ${parmPrefix}*")
                                                            }
                                                            JSON.convertValue(tempMap, param.type)
                                                        } else {
                                                            try {
                                                                JSON.readValue(routeContext.getBodyAsString(), param.type)
                                                            } catch (ex: Throwable) {
                                                                throw RuntimeException("cannot bind parameter ${param.name} from incoming data, expected valid JSON.  Failed due to ${ex.getMessage()}", ex)
                                                            }
                                                        }
                                                    } else {
                                                        // TODO: how does this handl nulls?
                                                        JSON.convertValue(request.getParam(param.name), param.type)
                                                    }
                                                    useValues.add(paramValue)
                                                }

                                                val result = invokeMethod.invoke(instance, useValues)
                                            }

                                        } else {
                                            throw RuntimeException("Invalid property ${prop.name} on class ${controller.getCanonicalName()} has HTTP verb but cannot find an invokable function in the extension method synthesized class")
                                        }
                                    } else {
                                        throw RuntimeException("Invalid property ${prop.name} on class ${controller.getCanonicalName()} has HTTP verb but is not an extension function literal or pointer to one")
                                    }
                                } else {
                                    if (typeNameOfField.startsWith("kotlin.Function") || typeNameOfField.startsWith("kotlin.MemberFunction")) {
                                        // TODO: also log message saying the function needs to be extension on a context object in case they just didn't know
                                    }
                                    throw RuntimeException("Invalid property ${prop.name} on class ${controller.getCanonicalName()} has HTTP verb but is not an extension function on a context object")
                                }
                            } else {
                                // ignore field that is not http verb
                            }
                        } else {
                            // LOG ignoring unknown property that has no backing field because it is too complicated to safely resolve what it actually is
                        }
                    }
                } else {
                    throw RuntimeException("Cannot use KapexController on a class that is not an object instance (object Xyz vs. class Xyz)")
                }
            }

            router.route("/test").handler { context ->
                context.response().putHeader("content-type", "text/html").end("Hello World!")
            }

            vertx.createHttpServer().requestHandler { router.accept(it) }.listen(8080)
            vertx.createHttpServer(HttpServerOptions()
                    .setSsl(true)
                    .setKeyStoreOptions(JksOptions().setPath("/Users/jminard/DEV/Collokia/ssl/keystore/keystore.jks").setPassword("g00fball")))
                    .requestHandler { router.accept(it) }.listen(8443)
        }
    }
}

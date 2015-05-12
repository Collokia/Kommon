package org.collokia.kommon.kapex

import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.datatype.jsr310.JSR310Module
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.vertx.core.AbstractVerticle
import io.vertx.core.VertxOptions
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.net.JksOptions
import io.vertx.ext.apex.Route
import io.vertx.ext.apex.Router
import io.vertx.ext.apex.RoutingContext
import io.vertx.ext.apex.handler.BodyHandler
import io.vertx.ext.apex.handler.CookieHandler
import io.vertx.ext.apex.handler.SessionHandler
import io.vertx.ext.apex.sstore.LocalSessionStore
import jet.runtime.typeinfo.JetValueParameter
import nl.mplatvoet.komponents.kovenant.async
import org.collokia.kommon.jdk.strings.*
import org.collokia.kommon.vertk.promiseDeployVerticle
import org.collokia.kommon.vertk.vertx
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import org.reflections.scanners.TypeAnnotationsScanner
import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
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
        // TODO: make intercept requests work
        if (target == indexGet) {
            println("I know who this is!!")
        }
        println("Intercept")
    }

    override fun interceptFailures(target: KCallable<Any>) {
        // TODO: make intercept failures work
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

    KapexRoute(HttpMethod.GET, "with/:something/show")
    val indexGetSomething = fun TestContext.(something: String): String {
        return something
    }

    KapexRoute(HttpMethod.POST, "edit")
    val editPost = fun TestContext.(xyz: Int, farf: Long) {
        throw KapexRedirect("/app/myform?xyz=${xyz}&farf=${farf}")
    }

    KapexRoute(HttpMethod.POST, "editError")
    val editPostFailure = fun TestContext.(xyz: Int, farf: Long) {
    }

    KapexRoute(HttpMethod.GET, "edit")
    val editView = fun TestContext.(xyz: Int, farf: Long): String {
        return "hello html"
    }

    KapexRoute(HttpMethod.GET, "json1", produces = "application/json")
    val someJson1 = fun TestContext.(xyz: Int, farf: Long): TestDataClass {
        return TestDataClass(xyz, farf)
    }

    KapexRoute(HttpMethod.GET, "json2")
    val someJson2 = fun TestContext.(xyz: Int, farf: Long): TestDataClass {
        return TestDataClass(xyz, farf)
    }

    class TestContext(private val from: RoutingContext) {

    }

    data class TestDataClass(val xyz: Int, val farf: Long, val created: DateTime = DateTime.now(DateTimeZone.UTC))

}


class KapexRedirect(val path: String) : Exception() {
    fun absolutePath(forContext: RoutingContext): String {
        // TODO: we need to build up a correct redirect URL to be happy
        return path;
    }
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

public class KapexVerticle() : AbstractVerticle() {
    private val JSON = jacksonObjectMapper()
            .registerModule(JodaModule())
            .registerModule(GuavaModule())
            .registerModule(JSR310Module())
            .registerModule(Jdk8Module())

    companion object {
        [platformStatic]
        public fun main(args: Array<String>) {
            val vertx = vertx(VertxOptions().setWorkerPoolSize(Runtime.getRuntime().availableProcessors() * 2)) success { vertx ->
                vertx.promiseDeployVerticle(KapexVerticle()) success { deploymentId ->
                    println("Deployed as $deploymentId")
                } fail { failureException ->
                    println("Failed due to $failureException")
                }
            } fail { failureException ->
                println("Failed due to $failureException")
            }
        }
    }

    private object EmptyContextFactory : ContextFactory<RoutingContext> {
        override fun createContext(from: RoutingContext): RoutingContext {
            return from
        }
    }

    override fun start() {
        async {
            val router = Router.router(vertx)
            router.route().handler(CookieHandler.create())
            router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)).setSessionTimeout(TimeUnit.HOURS.toMillis(12)).setNagHttps(false)) // TODO: if under a load balancer it might be secured to the front-end

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

                                            val paramDefs = paramNames.zip(paramTypes).map { ParamDef(it.first, it.second) }
                                            val paramContainsComplex = paramDefs.none { isSimpleDataType(it.type) }

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

                                            routeWithDatabinding(route, instance, memberFunction, invokeMethod, returnType, paramDefs, contextFactory,
                                                    routeAnnotation.accepts, routeAnnotation.produces)

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

            vertx.createHttpServer().requestHandler { router.accept(it) }.listen(8080)
            vertx.createHttpServer(HttpServerOptions()
                    .setSsl(true)
                    .setKeyStoreOptions(JksOptions().setPath("/Users/jminard/DEV/Collokia/ssl/keystore/keystore.jks").setPassword("g00fball")))
                    .requestHandler { router.accept(it) }.listen(8443)

            // TODO: set ready in countdown latch?
            println("Server ready, listening on HTTP 8080 and HTTPS 8443")
        }
    }

    private fun routeWithDatabinding(route: Route, controllerInstance: Any?, memberFunctionObjectInstance: Any?,
                                     memberFunctionInvokeMethod: Method, returnType: Class<*>,
                                     paramDefs: List<ParamDef>, contextFactory: ContextFactory<*>,
                                     acceptsContentType: String, producesContentType: String) {
        route.handler { routeContext ->
            val requestContext = contextFactory.createContext(routeContext)

            val request = routeContext.request()
            val useValues = linkedListOf<Any?>()
            paramDefs.forEach { param ->
                val paramValue: Any? = if (isSimpleDataType(param.type)) {
                    // TODO: how does this handle nulls and missing params?
                    JSON.convertValue(request.getParam(param.name), param.type)
                } else {
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
                }
                useValues.add(paramValue)
            }

            useValues.addFirst(requestContext) // put the $receiver on the front
            try {
                val result = memberFunctionInvokeMethod.invoke(memberFunctionObjectInstance, *useValues.toArray())
                if (returnType.getName() == "void") {
                    throw RuntimeException("Failutre after invocation of route function:  A route without a return type must redirect.")
                } else if (returnType.isAssignableFrom(javaClass<String>())) {
                    val contentType = routeContext.getAcceptableContentType() ?: producesContentType.nullIfBlank() ?: "text/html"
                    if (result == null) {
                        throw RuntimeException("Handler did not return any content, only a null which for HTML doesn't really make sense.")
                    }
                    routeContext.response().putHeader("content-type", contentType).end(result as String)
                } else {
                    // at this point we really just need to make a JSON object because we have data not text

                    // TODO: should we check if getAcceptableContentType() conflicts with application/json
                    // TODO: should we check if the produces content type conflicts with application/json
                    // TODO: we now return what they want as content type, but we are really creating JSON
                    val contentType = routeContext.getAcceptableContentType() ?: producesContentType.nullIfBlank() ?: "application/json"
                    routeContext.response().putHeader("content-type", contentType).end(JSON.writeValueAsString(result))
                }
            } catch (ex: InvocationTargetException) {
                try {
                    throw ex.getCause()
                } catch (redirect: KapexRedirect) {
                    routeContext.response().putHeader("location", redirect.absolutePath(routeContext)).setStatusCode(302).end()
                }
            }
        }
    }

    [data] class ParamDef(val name: String, val type: Class<*>)
}

private fun isSimpleDataType(type: Class<*>) = simpleDataTypes.any { type.isAssignableFrom(it) } || simpleTypeNames.contains(type.getName())
private val simpleDataTypes = listOf(javaClass<Boolean>(), javaClass<Number>(), javaClass<String>(), javaClass<DateTime>(), javaClass<Date>(),
        javaClass<java.lang.Integer>(), javaClass<java.lang.Long>(), javaClass<java.lang.Float>(), javaClass<java.lang.Double>(), javaClass<java.lang.Boolean>())
private val simpleTypeNames = setOf("int", "long", "float", "double", "boolean")

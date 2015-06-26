package org.collokia.kommon.vertx.web.helper;

import kotlin.reflect.jvm.internal.KClassImpl;
import kotlin.reflect.jvm.internal.impl.descriptors.ClassDescriptor;

class ReflectionUtil {
    static ClassDescriptor getClassDescriptor(KClassImpl<?> klass) {
        return klass.getDescriptor();
    }
}
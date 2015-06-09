package org.collokia.kommon.kapex.helper;

import kotlin.reflect.jvm.internal.KClassImpl;
import org.jetbrains.kotlin.descriptors.ClassDescriptor;

class ReflectionUtil {
    static ClassDescriptor getClassDescriptor(KClassImpl<?> klass) {
        return klass.getDescriptor();
    }
}
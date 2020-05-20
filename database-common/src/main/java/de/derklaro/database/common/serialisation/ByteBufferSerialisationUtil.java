/*
 * MIT License
 *
 * Copyright (c) Pasqual Koschmieder
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package de.derklaro.database.common.serialisation;

import de.derklaro.database.api.buffer.ByteBuffer;
import de.derklaro.database.api.objects.DatabaseObject;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationTargetException;

public final class ByteBufferSerialisationUtil {

    private ByteBufferSerialisationUtil() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    public static <V extends DatabaseObject> byte[] serialize(@NotNull V object) {
        ByteBuffer byteBuffer = null;
        try {
            byteBuffer = new ByteBuffer(Unpooled.buffer());
            object.serialize(byteBuffer);

            return byteBuffer.toByteArray();
        } finally {
            if (byteBuffer != null) {
                byteBuffer.release();
            }
        }
    }

    @NotNull
    public static <V extends DatabaseObject> V deserialize(@NotNull byte[] source, @NotNull Class<V> type) {
        ByteBuffer byteBuffer = null;
        try {
            V instance = type.getDeclaredConstructor().newInstance();
            byteBuffer = new ByteBuffer(Unpooled.wrappedBuffer(source));
            instance.deserialize(byteBuffer);

            return instance;
        } catch (final NoSuchMethodException exception) {
            throw new RuntimeException("Missing NoArgsConstructor in object class " + type.getName(), exception);
        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException exception) {
            throw new RuntimeException("Unable to deserialize database object " + type.getName(), exception);
        } finally {
            if (byteBuffer != null) {
                byteBuffer.release();
            }
        }
    }
}

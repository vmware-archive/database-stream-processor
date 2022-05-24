/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
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
 *
 *
 */

package org.dbsp.util;/*
 * Copyright (c) 2021 VMware, Inc.
 * SPDX-License-Identifier: MIT
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
 *
 */

import javax.annotation.Nullable;
import java.util.*;

public class Utilities {
    public static Set<String> makeSet(String... data) {
        return new HashSet<String>(Arrays.asList(data));
     }

     @SafeVarargs
     public static <T> List<T> concatenate(List<T>... lists) {
        ArrayList<T> result = new ArrayList<T>();
        for (List<T> l: lists)
            result.addAll(l);
        return result;
     }

    public static <K, V> void copyMap(Map<K, V> destination, Map<K, V> source) {
         destination.putAll(source);
     }

     public static String singleQuote(String other) {
         return "'" + other + "'";
     }

    /**
     * put something in a hashmap that is supposed to be new.
     * @param map    Map to insert in.
     * @param key    Key to insert in map.
     * @param value  Value to insert in map.
     * @return       The inserted value.
     */
    public static <K, V, VE extends V> VE putNew(@Nullable Map<K, V> map, K key, VE value) {
        V previous = Objects.requireNonNull(map).put(Objects.requireNonNull(key), Objects.requireNonNull(value));
        assert previous == null : "Key " + key + " already mapped to " + previous + " when adding " + value;
        return value;
    }

    /**
     * Get a value that must exist in a map.
     * @param map  Map to look for.
     * @param key  Key the value is indexed with.
     */
    public static <K, V> V getExists(Map<K, V> map, K key) {
        V result = map.get(key);
        assert result != null : "Key " + key + " does not exist in map";
        return result;
    }
}

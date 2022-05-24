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


import java.util.List;

public class IndentStringBuilder {
    private final StringBuilder builder;
    int indent = 0;
    static final int amount = 4;
    boolean emitIndent = false;

    public IndentStringBuilder() {
        this.builder = new StringBuilder();
    }

    public void appendChar(char c) {
        if (c == '\n') {
            this.builder.append(c);
            this.emitIndent = true;
            return;
        }
        if (this.emitIndent && !Character.isSpaceChar(c)) {
            this.emitIndent = false;
            for (int in = 0; in < this.indent; in++)
                this.builder.append(' ');
        }
        this.builder.append(c);
    }

    public IndentStringBuilder append(String string) {
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            this.appendChar(c);
        }
        return this;
    }

    public IndentStringBuilder append(int value) {
        this.builder.append(value);
        return this;
    }

    public <T extends Printable> IndentStringBuilder join(String separator, T[] data) {
        boolean first = true;
        for (Printable d: data) {
            if (!first)
                this.append(separator);
            first = false;
            this.append(d);
        }
        return this;
    }

    public <T extends Printable> IndentStringBuilder join(String separator, List<T> data) {
        boolean first = true;
        for (Printable d: data) {
            if (!first)
                this.append(separator);
            first = false;
            this.append(d);
        }
        return this;
    }

    public IndentStringBuilder newline() {
        return this.append("\n");
    }

    public IndentStringBuilder append(Printable object) {
        return object.toRustString(this);
    }

    public IndentStringBuilder increase() {
        this.indent += amount;
        return this.newline();
    }

    public IndentStringBuilder decrease() {
        this.indent -= amount;
        if (this.indent < 0)
            throw new RuntimeException("Negative indent");
        return this;
    }

    @Override
    public String toString() {
        return this.builder.toString();
    }
}

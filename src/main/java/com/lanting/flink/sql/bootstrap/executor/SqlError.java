/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.lanting.flink.sql.bootstrap.executor;

import lombok.Getter;

/**
 * 带源码位置信息的 SQL 异常。
 *
 * <p>参考 Calcite {@code SqlParserPos} 的设计，记录错误的起始行号/列号，
 * 在异常消息中清晰呈现错误位置。
 */
@Getter
public class SqlError extends RuntimeException {

    private final int lineNumber;
    private final int columnNumber;

    public SqlError(int line, int column, String message) {
        super(format(line, column, message));
        this.lineNumber = line;
        this.columnNumber = column;
    }

    public SqlError(int line, int column, String message, Throwable cause) {
        super(format(line, column, message), cause);
        this.lineNumber = line;
        this.columnNumber = column;
    }

    private static String format(int line, int col, String message) {
        return String.format("Line %d, Column %d: %s", line, col, message);
    }
}

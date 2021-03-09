/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.client.util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that splits a (large) {@link List} into a set of smaller batches (also represented as {@link List}).
 *
 * This class offers a performant way of splitting collections into smaller batches. Depending on input size it may
 * be 10 times more efficient than a for loop.
 *
 * @param <T>
 */
public final class Partition<T> extends AbstractList<List<T>> {

    private final List<T> list;
    private final int batchSize;

    public Partition(List<T> list, int batchSize) {
        this.list = new ArrayList<>(list);
        this.batchSize = batchSize;
    }

    public static <T> Partition<T> ofSize(List<T> list, int batchSize) {
        return new Partition<>(list, batchSize);
    }

    @Override
    public List<T> get(int index) {
        int start = index * batchSize;
        int end = Math.min(start + batchSize, list.size());

        if (start > end) {
            throw new IndexOutOfBoundsException("Index " + index + " is out of the list range <0," + (size() - 1) + ">");
        }

        return new ArrayList<>(list.subList(start, end));
    }

    @Override
    public int size() {
        return (int) Math.ceil((double) list.size() / (double) batchSize);
    }
}
package com.cognite.client.stream;

import com.cognite.client.Request;

import java.util.Iterator;
import java.util.List;

/**
 * An interface for API endpoints that supports iterating over a results stream based on a
 * {@link Request} specification.
 *
 * @param <T> The type of the resulting object read from CDF.
 */
public interface ListSource<T> {

    /**
     * Returns all {@code T} objects that matches the filters set in the {@link Request}.
     *
     * The results are paged through / iterated over via an {@link Iterator}--the entire results set is not buffered in
     * memory, but streamed in "pages" from the Cognite api. If you need to buffer the entire results set, then you
     * have to stream these results into your own data structure.
     *
     * @param requestParameters the filters to use for retrieving the results objects
     * @return an {@link Iterator} to page through the results set.
     */
    public Iterator<List<T>> list(Request requestParameters) throws Exception;
}

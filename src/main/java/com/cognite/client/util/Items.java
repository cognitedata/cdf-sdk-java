package com.cognite.client.util;

import com.cognite.client.dto.Item;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class for helping build {@link Item} objects.
 */
public class Items {

    /**
     * Build a list of {@link Item} based on a set of {@code externaIds}.
     *
     * @param externalId The {@code externaIds} to use as a basis for the {@code Items}.
     * @return The corresponding list of {@link Item}
     */
    public static List<Item> parseItems(String... externalId) {
        return Arrays.stream(externalId)
                .map(extId -> Item.newBuilder().setExternalId(extId).build())
                .collect(Collectors.toList());
    }

    /**
     * Build a list of {@link Item} based on a set of {@code ids}.
     *
     * @param internalId The {@code ids} to use as a basis for the {@code Items}.
     * @return The corresponding list of {@link Item}
     */
    public static List<Item> parseItems(long... internalId) {
        return Arrays.stream(internalId)
                .mapToObj(id -> Item.newBuilder().setId(id).build())
                .collect(Collectors.toList());
    }
}

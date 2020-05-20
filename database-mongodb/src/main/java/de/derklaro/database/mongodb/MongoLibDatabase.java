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
package de.derklaro.database.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import de.derklaro.database.api.Database;
import de.derklaro.database.api.objects.DatabaseEntry;
import de.derklaro.database.api.objects.DatabaseObject;
import de.derklaro.database.common.entry.DefaultDatabaseEntry;
import de.derklaro.database.common.serialisation.ByteBufferSerialisationUtil;
import org.bson.Document;
import org.bson.types.Binary;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class MongoLibDatabase<V extends DatabaseObject> implements Database<V> {

    MongoLibDatabase(@NotNull MongoCollection<Document> collection, @NotNull Class<V> type) {
        this.collection = collection;
        this.type = type;
    }

    private final MongoCollection<Document> collection;

    private final Class<V> type;

    @Override
    public @NotNull CompletableFuture<Void> insert(@NotNull String key, @NotNull String identifier, @NotNull V value) {
        return CompletableFuture.supplyAsync(() -> {
            byte[] data = ByteBufferSerialisationUtil.serialize(value);

            Document document = new Document()
                    .append("_id", key)
                    .append("_internal_id", identifier)
                    .append("data", new Binary(data));
            this.collection.insertOne(document);
            return null;
        });
    }

    @Override
    public @NotNull CompletableFuture<Optional<V>> get(@NotNull String key, @Nullable String identifier) {
        return CompletableFuture.supplyAsync(() -> {
            Document document = this.collection.find(Filters.or(Filters.eq("_id", key), Filters.eq("_internal_id", identifier))).first();
            if (document == null) {
                return Optional.empty();
            }

            byte[] data = document.get("data", Binary.class).getData();
            return Optional.of(ByteBufferSerialisationUtil.deserialize(data, this.type));
        });
    }

    @Override
    public @NotNull CompletableFuture<Void> updateIdentifier(@NotNull String key, @NotNull String identifier) {
        return CompletableFuture.supplyAsync(() -> {
            Document document = this.collection.find(Filters.eq("_id", key)).first();
            if (document == null) {
                return null;
            }

            this.collection.updateOne(Filters.eq("_id", key), new Document().append("_internal_id", identifier));
            return null;
        });
    }

    @Override
    public @NotNull CompletableFuture<Void> remove(@NotNull String key) {
        return CompletableFuture.supplyAsync(() -> {
            this.collection.deleteOne(Filters.eq("_id", key));
            return null;
        });
    }

    @Override
    public @NotNull CompletableFuture<Void> removeAll(@NotNull String identifier) {
        return CompletableFuture.supplyAsync(() -> {
            this.collection.deleteMany(Filters.eq("_internal_id", identifier));
            return null;
        });
    }

    @Override
    public @NotNull CompletableFuture<Collection<V>> sortByIdentifier(int limit) {
        return CompletableFuture.supplyAsync(() -> {
            Collection<V> result = new ArrayList<>();
            for (Document document : this.collection.find().sort(new Document().append("_internal_id", -1)).limit(limit)) {
                byte[] data = document.get("data", Binary.class).getData();
                result.add(ByteBufferSerialisationUtil.deserialize(data, this.type));
            }

            return result;
        });
    }

    @Override
    public @NotNull CompletableFuture<Collection<String>> getKeys() {
        return CompletableFuture.supplyAsync(() -> {
            Collection<String> result = new ArrayList<>();
            for (Document document : this.collection.find()) {
                result.add(document.getString("_id"));
            }

            return result;
        });
    }

    @Override
    public @NotNull CompletableFuture<Collection<DatabaseEntry<V>>> getEntries() {
        return this.getEntriesFiltered(ignored -> true);
    }

    @Override
    public @NotNull CompletableFuture<Collection<DatabaseEntry<V>>> getEntries(@NotNull Predicate<String> identifierFilter) {
        return this.getEntriesFiltered(entry -> identifierFilter.test(entry.getIdentifier()));
    }

    @Override
    public @NotNull CompletableFuture<Collection<DatabaseEntry<V>>> getEntriesFiltered(@NotNull Predicate<DatabaseEntry<V>> entryFilter) {
        return CompletableFuture.supplyAsync(() -> {
            Collection<DatabaseEntry<V>> result = new ArrayList<>();
            for (Document document : this.collection.find()) {
                DatabaseEntry<V> databaseEntry = new DefaultDatabaseEntry<>(
                        document.getString("_id"),
                        document.getString("_internal_id"),
                        ByteBufferSerialisationUtil.deserialize(document.get("data", Binary.class).getData(), this.type),
                        this
                );

                if (entryFilter.test(databaseEntry)) {
                    result.add(databaseEntry);
                }
            }

            return result;
        });
    }

    @Override
    public @NotNull CompletableFuture<Stream<DatabaseEntry<V>>> stream() {
        return this.getEntries().handleAsync((result, th) -> {
            if (result == null) {
                throw new RuntimeException(th);
            }

            return result.stream();
        });
    }

    @Override
    public @NotNull CompletableFuture<Void> clear() {
        return CompletableFuture.supplyAsync(() -> {
            this.collection.deleteMany(new Document());
            return null;
        });
    }

    @Override
    public @NotNull CompletableFuture<Long> getSize() {
        return CompletableFuture.supplyAsync(() -> this.collection.countDocuments(new Document()));
    }

    @Override
    public @NotNull CompletableFuture<Iterator<V>> iterator() {
        return this.stream().handleAsync((result, th) -> {
            if (result == null) {
                throw new RuntimeException(th);
            }

            return result.map(DatabaseEntry::getEntry).iterator();
        });
    }

    @Override
    public @NotNull CompletableFuture<Spliterator<V>> spliterator() {
        return this.stream().handleAsync((result, th) -> {
            if (result == null) {
                throw new RuntimeException(th);
            }

            return result.map(DatabaseEntry::getEntry).spliterator();
        });
    }

    @Override
    public @NotNull CompletableFuture<Void> forEach(@NotNull Consumer<V> consumer) {
        return this.stream().handleAsync((result, th) -> {
            if (result == null) {
                throw new RuntimeException(th);
            }

            result.map(DatabaseEntry::getEntry).forEach(consumer);
            return null;
        });
    }
}

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
package de.derklaro.database.mysql;

import de.derklaro.database.api.Database;
import de.derklaro.database.api.objects.DatabaseEntry;
import de.derklaro.database.api.objects.DatabaseObject;
import de.derklaro.database.common.entry.DefaultDatabaseEntry;
import de.derklaro.database.common.serialisation.ByteBufferSerialisationUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MySQLDatabase<V extends DatabaseObject> implements Database<V> {

    MySQLDatabase(@NotNull MySQLDatabaseProvider provider, @NotNull String name, @NotNull Class<V> type) {
        this.provider = provider;
        this.name = name;
        this.type = type;

        provider.executeUpdate("CREATE TABLE IF NOT EXISTS `" + name + "` (`key` TEXT, `identifier` TEXT, `data` LONGBLOB);");
    }

    private final MySQLDatabaseProvider provider;
    private final String name;
    private final Class<V> type;

    @Override
    public @NotNull CompletableFuture<Void> insert(@NotNull String key, @NotNull String identifier, @NotNull V value) {
        return CompletableFuture.supplyAsync(() -> {
            byte[] data = ByteBufferSerialisationUtil.serialize(value);
            if (this.provider.executeUpdate("INSERT INTO " + this.name + " (`key`, `identifier`, `data`) VALUES (?, ?, ?)", key, identifier, data, 3) != -1) {
                return null;
            }

            this.provider.executeUpdate("UPDATE " + this.name + " SET `data` = ? WHERE `key` = ? OR `identifier` = ?", key, identifier, data, 1);
            return null;
        });
    }

    @Override
    public @NotNull CompletableFuture<Optional<V>> get(@NotNull String key, @Nullable String identifier) {
        return CompletableFuture.supplyAsync(() -> this.provider.executeQuery(resultSet -> {
            if (resultSet.next()) {
                byte[] data = resultSet.getBytes("data");
                if (data == null) {
                    return Optional.empty();
                }

                return Optional.of(ByteBufferSerialisationUtil.deserialize(data, this.type));
            }

            return Optional.empty();
        }, "SELECT data FROM " + this.name + " WHERE key = " + key + (identifier == null ? "" : " OR identifier = " + identifier)));
    }

    @Override
    public @NotNull CompletableFuture<Void> updateIdentifier(@NotNull String key, @NotNull String identifier) {
        return CompletableFuture.supplyAsync(() -> {
            this.provider.executeUpdate("UPDATE " + this.name + " SET `identifier` = `" + identifier + "` WHERE `key` = `" + key + "`");
            return null;
        });
    }

    @Override
    public @NotNull CompletableFuture<Void> remove(@NotNull String key) {
        return CompletableFuture.supplyAsync(() -> {
            this.provider.executeUpdate("DELETE FROM " + this.name + " WHERE key = `" + key + "`");
            return null;
        });
    }

    @Override
    public @NotNull CompletableFuture<Void> removeAll(@NotNull String identifier) {
        return CompletableFuture.supplyAsync(() -> {
            this.provider.executeUpdate("DELETE FROM " + this.name + " WHERE `identifier` = `" + identifier + "`");
            return null;
        });
    }

    @Override
    public @NotNull CompletableFuture<Collection<V>> sortByIdentifier(int limit) {
        return CompletableFuture.supplyAsync(() -> this.provider.executeQuery(resultSet -> {
            Collection<V> results = new ArrayList<>();
            while (resultSet.next()) {
                byte[] data = resultSet.getBytes("data");
                if (data == null) {
                    continue;
                }

                V instance = ByteBufferSerialisationUtil.deserialize(data, this.type);
                results.add(instance);
            }

            return results;
        }, "SELECT * FROM " + this.name + " ORDER BY `identifier`+0 LIMIT " + limit));
    }

    @Override
    public @NotNull CompletableFuture<Collection<String>> getKeys() {
        return CompletableFuture.supplyAsync(() -> this.provider.executeQuery(resultSet -> {
            Collection<String> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getString("key"));
            }

            return result;
        }, "SELECT `key` FROM " + this.name));
    }

    @Override
    public @NotNull CompletableFuture<Collection<DatabaseEntry<V>>> getEntries() {
        return CompletableFuture.supplyAsync(() -> this.provider.executeQuery(resultSet -> {
            Collection<DatabaseEntry<V>> results = new ArrayList<>();
            while (resultSet.next()) {
                byte[] data = resultSet.getBytes("data");
                if (data == null) {
                    continue;
                }

                V instance = ByteBufferSerialisationUtil.deserialize(data, this.type);
                results.add(new DefaultDatabaseEntry<>(resultSet.getString("key"), resultSet.getString("identifier"), instance, this));
            }

            return results;
        }, "SELECT * FROM " + this.name));
    }

    @Override
    public @NotNull CompletableFuture<Collection<DatabaseEntry<V>>> getEntries(@NotNull Predicate<String> identifierFilter) {
        return this.getEntries().handleAsync((result, th) -> {
            if (result == null) {
                return new ArrayList<>();
            }

            return result.stream().filter(entry -> identifierFilter.test(entry.getIdentifier())).collect(Collectors.toList());
        });
    }

    @Override
    public @NotNull CompletableFuture<Collection<DatabaseEntry<V>>> getEntriesFiltered(@NotNull Predicate<DatabaseEntry<V>> entryFilter) {
        return this.getEntries().handleAsync((result, th) -> {
            if (result == null) {
                return new ArrayList<>();
            }

            return result.stream().filter(entryFilter).collect(Collectors.toList());
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
            this.provider.executeUpdate("TRUNCATE TABLE " + this.name);
            return null;
        });
    }

    @Override
    public @NotNull CompletableFuture<Long> getSize() {
        return CompletableFuture.supplyAsync(() -> this.provider.executeQuery(resultSet -> {
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }

            return -1L;
        }, "SELECT COUNT(`key`) FROM `" + this.name + "`"));
    }

    @Override
    public @NotNull CompletableFuture<Iterator<V>> iterator() {
        return this.getEntries().handleAsync((result, th) -> {
            if (result == null) {
                throw new RuntimeException(th);
            }

            return result.stream().map(DatabaseEntry::getEntry).iterator();
        });
    }

    @Override
    public @NotNull CompletableFuture<Spliterator<V>> spliterator() {
        return this.getEntries().handleAsync((result, th) -> {
            if (result == null) {
                throw new RuntimeException(th);
            }

            return result.stream().map(DatabaseEntry::getEntry).spliterator();
        });
    }

    @Override
    public @NotNull CompletableFuture<Void> forEach(@NotNull Consumer<V> consumer) {
        return this.getEntries().thenAcceptAsync(result -> {
            for (DatabaseEntry<V> vDatabaseEntry : result) {
                consumer.accept(vDatabaseEntry.getEntry());
            }
        });
    }
}

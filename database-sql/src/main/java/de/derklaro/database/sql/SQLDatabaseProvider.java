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
package de.derklaro.database.sql;

import de.derklaro.database.api.Database;
import de.derklaro.database.api.DatabaseProvider;
import de.derklaro.database.api.objects.DatabaseObject;
import de.derklaro.database.sql.util.SQLExceptionFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public abstract class SQLDatabaseProvider implements DatabaseProvider {

    protected final Map<Class<? extends DatabaseObject>, Database<? extends DatabaseObject>> databases = new ConcurrentHashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <V extends DatabaseObject> Database<V> getDatabase(@NotNull String name, @NotNull Class<V> type) {
        if (this.databases.containsKey(type)) {
            return (Database<V>) this.databases.get(type);
        }

        Database<V> database = new SQLDatabase<>(this, name, type);
        this.databases.put(type, database);
        return database;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> existsDatabase(@NotNull String name) {
        return CompletableFuture.supplyAsync(() -> this.executeQuery(
                ResultSet::next, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = " + name + " LIMIT 1"
        ));
    }

    @Override
    public @NotNull CompletableFuture<Boolean> deleteDatabase(@NotNull String name) {
        return CompletableFuture.supplyAsync(() -> this.executeUpdate("DROP TABLE " + name) != -1);
    }

    @Override
    public @NotNull CompletableFuture<Collection<String>> getDatabaseNames() {
        return CompletableFuture.supplyAsync(() -> this.executeQuery(resultSet -> {
            Collection<String> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getString("table_name"));
            }

            return result;
        }, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC'"));
    }

    protected abstract int executeUpdate(@NotNull String query, @NotNull String key, @NotNull String identifier, @NotNull byte[] data, int dataIndex);

    protected abstract int executeUpdate(@NotNull String query, @NotNull Object... objects);

    @Nullable
    protected abstract <T> T executeQuery(@NotNull SQLExceptionFunction<ResultSet, T> consumer, @NotNull String query, @NotNull Object... objects);
}

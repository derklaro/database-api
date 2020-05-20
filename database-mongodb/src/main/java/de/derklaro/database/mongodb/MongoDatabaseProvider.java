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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import de.derklaro.database.api.Database;
import de.derklaro.database.api.DatabaseProvider;
import de.derklaro.database.api.objects.DatabaseObject;
import de.derklaro.database.common.NameSpaceValidator;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class MongoDatabaseProvider implements DatabaseProvider {

    private final Map<Class<? extends DatabaseObject>, Database<? extends DatabaseObject>> databases = new ConcurrentHashMap<>();

    public MongoDatabaseProvider(@NotNull MongoClient mongoClient, @NotNull MongoDatabase mongoDatabase) {
        this.mongoClient = mongoClient;
        this.mongoDatabase = mongoDatabase;
    }

    private final MongoDatabase mongoDatabase;

    private final MongoClient mongoClient;

    @Override
    public @NotNull CompletableFuture<Boolean> isConnected() {
        return CompletableFuture.completedFuture(true); // todo: find a way to check this
    }

    @Override
    public @NotNull CompletableFuture<Boolean> closeConnection() {
        return CompletableFuture.supplyAsync(() -> {
            this.mongoClient.close();
            return true;
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <V extends DatabaseObject> Database<V> getDatabase(@NotNull String name, @NotNull Class<V> type) {
        NameSpaceValidator.validateNameSpace(name);
        if (this.databases.containsKey(type)) {
            return (Database<V>) this.databases.get(type);
        }

        Database<V> database = new MongoLibDatabase<>(this.mongoDatabase.getCollection(name), type);
        this.databases.put(type, database);
        return database;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> existsDatabase(@NotNull String name) {
        return CompletableFuture.supplyAsync(() -> {
            for (String s : this.mongoClient.listDatabaseNames()) {
                if (s.equals(name)) {
                    return true;
                }
            }

            return false;
        });
    }

    @Override
    public @NotNull CompletableFuture<Boolean> deleteDatabase(@NotNull String name) {
        return CompletableFuture.supplyAsync(() -> {
            this.mongoDatabase.getCollection(name).drop();
            return true;
        });
    }

    @Override
    public @NotNull CompletableFuture<Collection<String>> getDatabaseNames() {
        return CompletableFuture.supplyAsync(() -> {
            Collection<String> result = new ArrayList<>();
            for (String s : this.mongoClient.listDatabaseNames()) {
                result.add(s);
            }

            return result;
        });
    }
}

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
package de.derklaro.database.mongodb.connection;

import de.derklaro.database.api.DatabaseProvider;
import de.derklaro.database.api.connection.ConnectionConfiguration;
import de.derklaro.database.api.connection.ConnectionProvider;
import de.derklaro.database.mongodb.MongoDatabaseProvider;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

public class MongoConnectionProvider implements ConnectionProvider {

    private final Collection<DatabaseProvider> databaseProviders = new CopyOnWriteArrayList<>();

    @Override
    public @NotNull CompletableFuture<Optional<DatabaseProvider>> connect(@NotNull ConnectionConfiguration connectionConfiguration) {
        return CompletableFuture.supplyAsync(() -> {
            String authParams = !connectionConfiguration.getUserName().isEmpty() && !connectionConfiguration.getPassword().isEmpty()
                    ? connectionConfiguration.getUserName() + ":" + connectionConfiguration.getPassword() + "@"
                    : "";

            MongoClient client = MongoClients.create(new ConnectionString(
                    "mongodb://" + authParams + connectionConfiguration.getHost() + ":" + connectionConfiguration.getPort()
                            + "/" + connectionConfiguration.getTargetDatabase() + "?ssl=" + connectionConfiguration.useSSL() + "&sslInvalidHostNameAllowed=true"
            ));
            DatabaseProvider result = new MongoDatabaseProvider(client, client.getDatabase(connectionConfiguration.getTargetDatabase()));
            this.databaseProviders.add(result);
            return Optional.of(result);
        });
    }

    @Override
    public @NotNull CompletableFuture<Void> closeAllConnections() {
        return CompletableFuture.supplyAsync(() -> {
            for (DatabaseProvider databaseProvider : this.databaseProviders) {
                databaseProvider.closeConnection().join();
            }

            this.databaseProviders.clear();
            return null;
        });
    }
}

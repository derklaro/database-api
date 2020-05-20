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
package de.derklaro.database.h2;

import de.derklaro.database.api.DatabaseProvider;
import de.derklaro.database.api.connection.ConnectionConfiguration;
import de.derklaro.database.sql.connection.SQLConnectionProvider;
import org.h2.Driver;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class H2ConnectionProvider extends SQLConnectionProvider {

    static {
        Driver.load();
    }

    @Override
    public @NotNull CompletableFuture<Optional<DatabaseProvider>> connect(@NotNull ConnectionConfiguration connectionConfiguration) {
        if (!connectionConfiguration.isLoaded()) {
            throw new RuntimeException("Can only connect to a database using a loaded connection configuration");
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                Connection connection = DriverManager.getConnection("jdbc:h2:" + new File(connectionConfiguration.getTargetDatabase()).getAbsolutePath());
                return Optional.of(new H2DatabaseProvider(connection));
            } catch (final SQLException exception) {
                exception.printStackTrace();
                return Optional.empty();
            }
        });
    }
}

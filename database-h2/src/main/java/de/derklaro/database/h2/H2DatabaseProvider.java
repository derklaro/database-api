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

import de.derklaro.database.sql.SQLDatabaseProvider;
import de.derklaro.database.sql.util.SQLExceptionFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

public class H2DatabaseProvider extends SQLDatabaseProvider {

    H2DatabaseProvider(@NotNull Connection connection) {
        this.connection = connection;
    }

    private final Connection connection;

    @Override
    protected int executeUpdate(@NotNull String query, @NotNull String key, @NotNull String identifier, @NotNull byte[] data, int dataIndex) {
        try (PreparedStatement statement = this.connection.prepareStatement(query)) {
            if (dataIndex == 1) {
                statement.setBytes(1, data);
                statement.setString(2, key);
                statement.setString(3, identifier);
            } else {
                statement.setString(1, key);
                statement.setString(2, identifier);
                statement.setBytes(3, data);
            }

            return statement.executeUpdate();
        } catch (final SQLException exception) {
            exception.printStackTrace();
        }

        return -1;
    }

    @Override
    protected int executeUpdate(@NotNull String query, @NotNull Object... objects) {
        try (PreparedStatement statement = this.connection.prepareStatement(query)) {
            int i = 1;
            for (Object object : objects) {
                statement.setString(i++, object.toString());
            }

            return statement.executeUpdate();
        } catch (final SQLException exception) {
            exception.printStackTrace();
        }

        return -1;
    }

    @Override
    protected <T> @Nullable T executeQuery(@NotNull SQLExceptionFunction<ResultSet, T> consumer, @NotNull String query, @NotNull Object... objects) {
        try (PreparedStatement statement = this.connection.prepareStatement(query)) {
            int i = 1;
            for (Object object : objects) {
                statement.setString(i++, object.toString());
            }

            try (ResultSet resultSet = statement.executeQuery()) {
                return consumer.apply(resultSet);
            }
        } catch (final SQLException exception) {
            exception.printStackTrace();
        }

        return null;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> isConnected() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return !connection.isClosed() && connection.isValid(250);
            } catch (final SQLException ex) {
                return false;
            }
        });
    }

    @Override
    public @NotNull CompletableFuture<Boolean> closeConnection() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                this.connection.close();
            } catch (final SQLException ignored) {
            }

            return this.isConnected().join();
        });
    }
}

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
package de.derklaro.database.config.gson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import de.derklaro.database.api.connection.ConnectionConfiguration;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;

public class GsonConnectionConfiguration implements ConnectionConfiguration {

    private static final ThreadLocal<Gson> GSON = ThreadLocal.withInitial(
            () -> new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().serializeNulls().create()
    );

    public GsonConnectionConfiguration(@NotNull Path path) {
        this.path = path;
    }

    public GsonConnectionConfiguration(Path path, String host, String username, String password, String targetDatabase, int port, boolean useSsl) {
        this.path = path;
        this.host = host;
        this.username = username;
        this.password = password;
        this.targetDatabase = targetDatabase;
        this.port = port;
        this.useSsl = useSsl;
    }

    private final transient Path path;

    private String host;
    private String username;
    private String password;
    private String targetDatabase;
    private int port = -1;
    private boolean useSsl;

    @Override
    public @NotNull CompletableFuture<ConnectionConfiguration> load() {
        return CompletableFuture.supplyAsync(() -> {
            if (Files.notExists(this.path)) {
                JsonObject object = new JsonObject();
                object.add("config", GSON.get().toJsonTree(this));

                try (Writer writer = new OutputStreamWriter(Files.newOutputStream(this.path, StandardOpenOption.CREATE), StandardCharsets.UTF_8)) {
                    GSON.get().toJson(object, writer);
                } catch (final IOException exception) {
                    throw new RuntimeException(exception);
                }
            }

            try (Reader reader = new InputStreamReader(Files.newInputStream(this.path), StandardCharsets.UTF_8)) {
                return GSON.get().fromJson(reader, GsonConnectionConfiguration.class);
            } catch (final IOException exception) {
                throw new RuntimeException(exception);
            }
        });
    }

    @Override
    public @NotNull String getHost() {
        return this.host;
    }

    @Override
    public @NotNull String getUserName() {
        return this.username;
    }

    @Override
    public @NotNull String getPassword() {
        return this.password;
    }

    @Override
    public @NotNull String getTargetDatabase() {
        return this.targetDatabase;
    }

    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public boolean useSSL() {
        return this.useSsl;
    }

    @Override
    public boolean isLoaded() {
        return this.host != null && this.username != null && this.targetDatabase != null && this.password != null && this.port > 0;
    }
}

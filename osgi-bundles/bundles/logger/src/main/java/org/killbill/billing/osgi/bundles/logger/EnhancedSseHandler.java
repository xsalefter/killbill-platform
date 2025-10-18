/*
 * Copyright 2020-2025 Equinix, Inc
 * Copyright 2014-2025 The Billing Project, LLC
 *
 * The Billing Project licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.killbill.billing.osgi.bundles.logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.jooby.Request;
import org.jooby.Sse;
import org.jooby.funzy.Throwing;
import org.killbill.commons.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnhancedSseHandler implements Sse.Handler, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(EnhancedSseHandler.class);

    private static final long INITIAL_DELAY_SECONDS = 1L;
    private static final long PERIOD_SECONDS = 1L;

    private final LogEntriesManager logEntriesManager;
    private final ScheduledExecutorService executor;

    public EnhancedSseHandler(final LogEntriesManager logEntriesManager) {
        this.logEntriesManager = logEntriesManager;
        this.executor = Executors.newSingleThreadScheduledExecutor("LogsSseHandler");
    }

    @Override
    public void close() throws IOException {
        executor.shutdownNow();
        logEntriesManager.close();
    }

    @Override
    public void handle(final Request req, final Sse sse) {
        final UUID cacheId = UUID.fromString(sse.id());
        final UUID lastEventId = sse.lastEventId(UUID.class).orElse(null);

        final SseSession session = new SseSession(cacheId, lastEventId, sse);
        session.start();
    }

    private final class SseSession {

        private final UUID cacheId;
        private final Sse sse;

        private final AtomicReference<UUID> lastLogId = new AtomicReference<>();
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicReference<ScheduledFuture<?>> taskRef = new AtomicReference<>();

        SseSession(final UUID cacheId, final UUID lastEventId, final Sse sse) {
            this.cacheId = cacheId;
            this.sse = sse;

            // Subscribe immediately so the cache is ready when the first drain runs.
            logEntriesManager.subscribe(cacheId, lastEventId);
        }

        void start() {
            // Schedule the drain loop with a short initial delay to let the SSE handshake complete.
            final ScheduledFuture<?> task = executor.scheduleAtFixedRate(
                    this::drainAndSend,
                    INITIAL_DELAY_SECONDS,
                    PERIOD_SECONDS,
                    TimeUnit.SECONDS);
            taskRef.set(task);

            sse.onClose(new Throwing.Runnable() {
                @Override
                public void tryRun() throws Throwable {
                    closeSession();
                }
            });
        }

        private void drainAndSend() {
            if (closed.get()) {
                return;
            }

            try {
                logger.debug("Draining SSE stream {}", cacheId);

                final Iterable<LogEntryJson> batch = logEntriesManager.drain(cacheId);
                final Iterator<LogEntryJson> iterator = batch.iterator();

                if (!iterator.hasNext()) {
                    // No new entries—emit heartbeat to keep connection alive and detect dead peers.
                    sendSafely(() -> sse.event("heartbeat").id(lastLogId.get()).send());
                    return;
                }

                while (iterator.hasNext()) {
                    final LogEntryJson entry = iterator.next();
                    sendSafely(() -> sse.event(entry).id(entry.getId()).send());
                    lastLogId.set(entry.getId());
                }
            } catch (final RuntimeException ex) {
                // Preserve the original behavior: bubble up unchecked exceptions after cleanup.
                closeSession();
                safeCloseSse();
                throw ex;
            }
        }

        private void sendSafely(final Throwing.Runnable action) {
            if (closed.get()) {
                return;
            }

            try {
                action.run();
            } catch (final IllegalStateException ex) {
                logger.debug("SSE stream {} is no longer writable, closing subscription", cacheId, ex);
                closeSession();
                safeCloseSse();
            } catch (final RuntimeException ex) {
                closeSession();
                safeCloseSse();
                throw ex;
            } catch (final Throwable ex) {
                closeSession();
                safeCloseSse();
                throw new RuntimeException(ex);
            }
        }

        private void closeSession() {
            if (!closed.compareAndSet(false, true)) {
                return; // already closed
            }

            final ScheduledFuture<?> task = taskRef.getAndSet(null);
            if (task != null) {
                task.cancel(true);
            }

            logEntriesManager.unsubscribe(cacheId);
        }

        private void safeCloseSse() {
            try {
                sse.close();
            } catch (final IllegalStateException ignore) {
                // Already closed by the container or another thread.
            } catch (final Exception e) {
                logger.warn("Failed to close SSE", e);
            }
        }
    }
}


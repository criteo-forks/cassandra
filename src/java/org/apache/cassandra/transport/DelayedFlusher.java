package org.apache.cassandra.transport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.*;
import org.apache.cassandra.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class DelayedFlusher implements Runnable
{
    protected static final Logger logger = LoggerFactory.getLogger(DelayedFlusher.class);
    public static final Integer FLUSH_DELAY = Integer.getInteger(Config.PROPERTY_PREFIX + "netty_flush_delay_nanoseconds", 10000);

    final EventLoop eventLoop;
    final ConcurrentLinkedQueue<FlushItem> queued = new ConcurrentLinkedQueue<>();
    final AtomicBoolean running = new AtomicBoolean(false);
    final HashSet<ChannelHandlerContext> channels = new HashSet<>();
    final List<FlushItem> flushed = new ArrayList<>();

    int runsSinceFlush = 0;
    int runsWithNoWork = 0;

    private static class FlushItem
    {
        final ChannelHandlerContext ctx;
        final Object response;
        final Frame sourceFrame;
        private FlushItem(ChannelHandlerContext ctx, Object response, Frame sourceFrame)
        {
            this.ctx = ctx;
            this.sourceFrame = sourceFrame;
            this.response = response;
        }
    }

    public DelayedFlusher(final EventLoop loop)
    {
        this.eventLoop = loop;
    }

    public void writeAndFlush(ChannelHandlerContext ctx, Object response, Frame sourceFrame) {
        queued.add(new FlushItem(ctx, response, sourceFrame));
        maybeStart();
    }

    void maybeStart()
    {
        if (!running.get() && running.compareAndSet(false, true))
        {
            this.eventLoop.execute(this);
        }
    }

    public void run() {

        if (doWork()) {
            runsWithNoWork = 0;
        } else {
            runsWithNoWork++;
        }

        if (runsWithNoWork > 5) {
            running.set(false);
            if (queued.isEmpty() || !running.compareAndSet(false, true))
                return;
        }

        if (FLUSH_DELAY > 0)
            eventLoop.schedule(this, FLUSH_DELAY, TimeUnit.NANOSECONDS);
        else
            eventLoop.execute(this);
    }

    private boolean doWork() {
        boolean doneWork = false;
        FlushItem flush;

        while ( null != (flush = queued.poll()) )
        {
            channels.add(flush.ctx);
            flush.ctx.write(flush.response, flush.ctx.voidPromise());
            flushed.add(flush);
            doneWork = true;
        }

        runsSinceFlush++;

        if (!doneWork || runsSinceFlush > 2 || flushed.size() > 50)
        {
            for (ChannelHandlerContext channel : channels)
                channel.flush();
            for (FlushItem item : flushed)
                item.sourceFrame.release();

            channels.clear();
            flushed.clear();
            runsSinceFlush = 0;
        }

        return doneWork;
    }
}


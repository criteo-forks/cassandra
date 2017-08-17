/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;


import java.util.Collections;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
public class TWCSCompactionController extends CompactionController
{
    private final boolean ignoreOverlaps;
    private static final Logger logger = LoggerFactory.getLogger(TWCSCompactionController.class);

    public TWCSCompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore, boolean ignoreOverlaps)
    {
        super(cfs, compacting, gcBefore);
        this.ignoreOverlaps = ignoreOverlaps;
        if(ignoreOverlaps)
            logger.warn("You are running with sstables overlapping checks disabled, it can result in loss of data");
    }

    @Override
    protected boolean ignoreOverlaps()
    {
        return ignoreOverlaps;
    }

    public static Set<SSTableReader> getFullyExpiredSSTables(ColumnFamilyStore cfs, Set<SSTableReader> uncompacting, int gcBefore, boolean ignoreOverlaps)
    {
        return CompactionController.getFullyExpiredSSTables(cfs, uncompacting, ignoreOverlaps ? Collections.emptySet()
                                                                                              : cfs.getOverlappingLiveSSTables(uncompacting),
                                                            gcBefore, ignoreOverlaps);
    }
}

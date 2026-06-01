/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.lanting.flink.sql.bootstrap.util;

import org.apache.flink.api.dag.Transformation;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

/**
 * DAG 打印机 V2：四阶段流水线，坐标只算一次不回头。
 */
@Experimental
class DAGPrinter {

    private static final int GAP   = 8;
    private static final int MIN_ROW_H = 4;  // 最小行高
    private static final int PAD   = 2;

    // ── 入口 ───────────────────────────────────────────

    public static void print(List<Transformation<?>> sinks) {
        // Phase 1: 收集、分行、标签
        Map<Integer, Transformation<?>> all = new LinkedHashMap<>();
        sinks.forEach(s -> collect(s, all));
        List<Transformation<?>> sorted = kahn(all);

        Map<Integer, Integer> depth = computeDepth(sorted);
        int maxDepth = depth.values().stream().mapToInt(x -> x).max().orElse(0);

        Map<Integer, List<Transformation<?>>> rows = groupByDepth(sorted, depth);
        Map<Integer, String> labels = computeLabels(all);

        // Phase 2: 纯坐标分配
        Map<Integer, Integer> cx = assignCoords(depth, rows, labels);

        // Phase 2.5: 算每层间距 — 有跨列连线时自动加高
        int[] spacing = computeLayerSpacing(depth, cx, sorted, maxDepth);

        // Phase 3: 算偏移
        int leftmost = Integer.MAX_VALUE, rightmost = 0, leftPad = 0;
        for (Map.Entry<Integer, Integer> e : cx.entrySet()) {
            int x = e.getValue();
            int half = labels.get(e.getKey()).length() / 2;
            if (x - half < leftmost) leftmost = x - half;
            if (x + half > rightmost) rightmost = x + half;
            if (half - x > leftPad) leftPad = half - x;
        }
        int baseShift = Math.max(0, leftPad - leftmost);
        for (int id : cx.keySet()) {
            cx.put(id, cx.get(id) + baseShift);
        }
        int postLeft = Integer.MAX_VALUE, postRight = 0;
        for (Map.Entry<Integer, Integer> e : cx.entrySet()) {
            int x = e.getValue();
            int half = labels.get(e.getKey()).length() / 2;
            if (x - half < postLeft) postLeft = x - half;
            if (x + half > postRight) postRight = x + half;
        }
        int contentW = postRight - postLeft;
        int width = contentW + 4;
        int centerShift = (width - contentW) / 2 - postLeft;
        for (int id : cx.keySet()) {
            cx.put(id, cx.get(id) + centerShift);
        }
        postRight = 0;
        for (Map.Entry<Integer, Integer> e : cx.entrySet()) {
            int right = e.getValue() + labels.get(e.getKey()).length() / 2;
            if (right > postRight) postRight = right;
        }
        width = postRight + 4;

        // 计算每层 y 坐标
        int[] yPos = new int[maxDepth + 1];
        yPos[0] = PAD;
        for (int d = 1; d <= maxDepth; d++) {
            yPos[d] = yPos[d - 1] + spacing[d - 1];
        }
        int height = yPos[maxDepth] + PAD;
        char[][] g = new char[height][width];
        for (char[] r : g) Arrays.fill(r, ' ');

        // Phase 4: 渲染 — 标签
        for (Transformation<?> t : sorted) {
            int x = cx.get(t.getId());
            int y = yPos[depth.get(t.getId())];
            String lbl = labels.get(t.getId());
            putStr(g, y, x - lbl.length() / 2, lbl);
        }

        // Phase 4: 渲染 — 连线
        for (Transformation<?> t : sorted) {
            int dx = cx.get(t.getId());
            int dy = yPos[depth.get(t.getId())];
            for (Transformation<?> inp : t.getInputs()) {
                if (!cx.containsKey(inp.getId())) continue;
                int sx = cx.get(inp.getId());
                int sy = yPos[depth.get(inp.getId())];
                boolean srcP = isPartition(inp);
                boolean dstP = isPartition(t);
                int rowH = dy - sy; // 实际行高
                drawEdge(g, sx, sy, dx, dy, rowH, srcP, dstP);
            }
        }

        System.out.println();
        for (char[] r : g) {
            String line = new String(r).stripTrailing();
            if (!line.isEmpty()) System.out.println(line);
        }
        int nodeCount = (int) all.values().stream().filter(t -> !isPartition(t)).count();
        System.out.printf("\nTotal: %d nodes\n", nodeCount);
    }

    // ── Phase 1 helpers ────────────────────────────────

    private static Map<Integer, Integer> computeDepth(List<Transformation<?>> sorted) {
        Map<Integer, Integer> depth = new LinkedHashMap<>();
        for (Transformation<?> t : sorted) {
            int d = t.getInputs().stream()
                    .filter(i -> depth.containsKey(i.getId()))
                    .mapToInt(i -> depth.get(i.getId()))
                    .max().orElse(-1) + 1;
            depth.put(t.getId(), d);
        }
        return depth;
    }

    private static Map<Integer, List<Transformation<?>>> groupByDepth(
            List<Transformation<?>> sorted, Map<Integer, Integer> depth) {
        Map<Integer, List<Transformation<?>>> rows = new LinkedHashMap<>();
        for (Transformation<?> t : sorted) {
            rows.computeIfAbsent(depth.get(t.getId()), k -> new ArrayList<>()).add(t);
        }
        return rows;
    }

    private static Map<Integer, String> computeLabels(Map<Integer, Transformation<?>> all) {
        Map<Integer, String> labels = new LinkedHashMap<>();
        for (Transformation<?> t : all.values()) {
            labels.put(t.getId(), nodeLabel(t));
        }
        return labels;
    }

    // ── Phase 2: 坐标分配 ──────────────────────────────

    private static Map<Integer, Integer> assignCoords(
            Map<Integer, Integer> depth,
            Map<Integer, List<Transformation<?>>> rows,
            Map<Integer, String> labels) {

        Map<Integer, Integer> cx = new LinkedHashMap<>();
        int maxDepth = depth.values().stream().mapToInt(x -> x).max().orElse(0);

        for (int d = 0; d <= maxDepth; d++) {
            List<Transformation<?>> row = rows.getOrDefault(d, Collections.emptyList());
            if (row.isEmpty()) continue;

            List<Transformation<?>> prevRow = d > 0 ? rows.get(d - 1) : null;
            boolean sameCount = prevRow != null && prevRow.size() == row.size();

            if (row.size() == 1) {
                // 单节点：对齐输入
                Transformation<?> t = row.get(0);
                int x = inputX(t, cx, 0);
                cx.put(t.getId(), x);

            } else if (sameCount) {
                // 同数量层：对齐输入 + 推开重叠
                int[] txs = new int[row.size()];
                for (int i = 0; i < row.size(); i++) {
                    txs[i] = inputX(row.get(i), cx, 0);
                }
                // 从左到右推开（保持拓扑序）
                int nextFree = 0;
                for (int i = 0; i < row.size(); i++) {
                    String lbl = labels.get(row.get(i).getId());
                    int half = lbl.length() / 2;
                    if (txs[i] - half < nextFree) txs[i] = nextFree + half;
                    nextFree = txs[i] + half + GAP;
                }
                for (int i = 0; i < row.size(); i++) {
                    cx.put(row.get(i).getId(), txs[i]);
                }

            } else {
                // 多节点不同数：左起排列
                int x = 0;
                for (Transformation<?> t : row) {
                    String lbl = labels.get(t.getId());
                    cx.put(t.getId(), x + lbl.length() / 2);
                    x += lbl.length() + GAP;
                }
            }
        }

        return cx;
    }

    /** 节点的理想 x：单输入对齐，多输入居中，无输入用 fallback */
    private static int inputX(Transformation<?> t, Map<Integer, Integer> cx, int fallback) {
        List<Integer> known = t.getInputs().stream()
                .filter(i -> cx.containsKey(i.getId()))
                .map(i -> cx.get(i.getId()))
                .collect(Collectors.toList());
        if (known.isEmpty()) return fallback;
        if (known.size() == 1) return known.get(0);
        return (int) known.stream().mapToInt(x -> x).average().orElse(fallback);
    }

    // ── Phase 2.5: 自适应行高 ──────────────────────────

    /** 计算层间距：有跨列连线时自动加高，保证横线上下都有 | */
    private static int[] computeLayerSpacing(Map<Integer, Integer> depth,
                                              Map<Integer, Integer> cx,
                                              List<Transformation<?>> sorted,
                                              int maxDepth) {
        int[] spacing = new int[maxDepth];
        Arrays.fill(spacing, MIN_ROW_H);

        for (Transformation<?> t : sorted) {
            int dd = depth.get(t.getId());
            int dx = cx.get(t.getId());
            for (Transformation<?> inp : t.getInputs()) {
                if (!cx.containsKey(inp.getId())) continue;
                int sd = depth.get(inp.getId());
                int sx = cx.get(inp.getId());
                int d = Math.min(sd, dd);
                int need = (sx != dx) ? 6 : 4;
                if (need > spacing[d]) spacing[d] = need;
            }
        }
        return spacing;
    }

    // ── Phase 4: 连线渲染 ──────────────────────────────

    private static void drawEdge(char[][] g, int sx, int sy, int dx, int dy, int rowH,
                                  boolean srcP, boolean dstP) {
        if (!srcP) put(g, sy + 1, sx, '+');
        int bot = dy - 1;
        if (sx == dx) {
            int y0 = srcP ? sy + 1 : sy + 2;
            vline(g, y0, bot - 1, sx);
            if (!dstP) put(g, bot, sx, 'v');
        } else {
            // 横线放在竖向空间中间，确保上下都有 |
            int mid = sy + 2 + (rowH - 3) / 2;
            int y0 = srcP ? sy + 1 : sy + 2;
            if (mid > y0) vline(g, y0, mid - 1, sx);
            hline(g, mid, sx, dx);
            put(g, mid, sx, '+');
            put(g, mid, dx, dx > sx ? '>' : '<');
            if (bot > mid + 1) vline(g, mid + 1, bot - 1, dx);
            if (!dstP) put(g, bot, dx, 'v');
        }
    }

    // ── 画布原语 ───────────────────────────────────────

    private static void put(char[][] g, int y, int x, char c) {
        if (y < 0 || y >= g.length || x < 0 || x >= g[0].length) return;
        char cur = g[y][x];
        if (cur == ' ' || c == '+' || c == 'v' || c == '<' || c == '>') g[y][x] = c;
    }

    private static void putStr(char[][] g, int y, int x, String s) {
        for (int i = 0; i < s.length(); i++) {
            int xi = x + i;
            if (xi >= 0 && xi < g[0].length && y >= 0 && y < g.length) g[y][xi] = s.charAt(i);
        }
    }

    private static void hline(char[][] g, int y, int x1, int x2) {
        int lo = Math.min(x1, x2), hi = Math.max(x1, x2);
        for (int x = lo; x <= hi; x++) if (g[y][x] == ' ') g[y][x] = '-';
    }

    private static void vline(char[][] g, int y0, int y1, int x) {
        for (int y = Math.min(y0, y1); y <= Math.max(y0, y1); y++)
            if (g[y][x] == ' ') g[y][x] = '|';
    }

    // ── 标签 ───────────────────────────────────────────

    private static boolean isPartition(Transformation<?> t) {
        return t.getClass().getSimpleName().equals("PartitionTransformation");
    }

    private static String nodeLabel(Transformation<?> t) {
        String cls = t.getClass().getSimpleName().replace("Transformation", "");
        if (cls.isEmpty()) cls = t.getClass().getSimpleName();
        if (cls.equals("Partition")) {
            try {
                Object p = t.getClass().getMethod("getPartitioner").invoke(t);
                if (p != null) {
                    String n = p.toString();
                    return StringUtils.isEmpty(n) ? cls : n + " Partition";
                }
            } catch (Exception ignored) {}
            return cls;
        }
        String name = t.getName();
        if (name == null || name.isBlank() || name.equalsIgnoreCase(cls) || name.contains("->"))
            return cls;
        return cls + ":" + name;
    }

    // ── 图算法 ─────────────────────────────────────────

    private static void collect(Transformation<?> t, Map<Integer, Transformation<?>> map) {
        if (map.containsKey(t.getId())) return;
        map.put(t.getId(), t);
        t.getInputs().forEach(i -> collect(i, map));
    }

    private static List<Transformation<?>> kahn(Map<Integer, Transformation<?>> all) {
        Map<Integer, Integer> indeg = new HashMap<>();
        Map<Integer, List<Integer>> out = new HashMap<>();
        all.keySet().forEach(id -> { indeg.put(id, 0); out.put(id, new ArrayList<>()); });
        all.values().forEach(t -> t.getInputs().forEach(i -> {
            indeg.merge(t.getId(), 1, Integer::sum);
            out.get(i.getId()).add(t.getId());
        }));
        Deque<Integer> q = new ArrayDeque<>();
        indeg.forEach((id, d) -> { if (d == 0) q.add(id); });
        List<Transformation<?>> res = new ArrayList<>();
        while (!q.isEmpty()) {
            int id = q.poll(); res.add(all.get(id));
            out.get(id).forEach(nx -> {
                if (indeg.merge(nx, -1, Integer::sum) == 0) q.add(nx);
            });
        }
        return res;
    }
}

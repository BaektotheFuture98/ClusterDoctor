"""컴파일된 LangGraph를 그대로 그린다.

손으로 그리지 않는다. ``get_graph().to_json()``이 돌려주는 노드와 엣지를 그대로
읽으므로, 그래프가 바뀌면 그림도 바뀐다. 외부 렌더러(mermaid.ink)를 쓰지
않아 네트워크 없이 돈다.
"""
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False

BG = "#0d1117"
FG = "#e6edf3"
MUTED = "#8b949e"
C_TERM = "#6e7681"   # __start__ / __end__
C_MODEL = "#8957e5"  # LLM이 도는 노드
C_TOOLS = "#238636"  # 도구 실행
C_MW = "#da3633"     # 미들웨어 훅


def _kind(node_id):
    if node_id in ("__start__", "__end__"):
        return C_TERM
    if node_id == "model":
        return C_MODEL
    if node_id == "tools":
        return C_TOOLS
    return C_MW


def _back_edges(nodes, edges, start="__start__"):
    """DFS로 순환을 만드는 엣지를 찾는다.

    층을 매기려면 먼저 DAG여야 한다. ``tools -> model``처럼 되돌아가는 엣지를
    그대로 두면 층 계산이 끝나지 않거나 엉뚱한 층이 나온다.
    """
    adj = {n: [] for n in nodes}
    for e in edges:
        if e["source"] in adj:
            adj[e["source"]].append(e["target"])

    back, state = set(), {}

    def visit(n):
        state[n] = "open"
        for t in adj.get(n, []):
            if state.get(t) == "open":
                back.add((n, t))
            elif t not in state:
                visit(t)
        state[n] = "done"

    if start in adj:
        visit(start)
    for n in nodes:
        if n not in state:
            visit(n)
    return back


def _layers(nodes, edges):
    """되돌아가는 엣지를 뺀 DAG에서 최장 경로로 층을 매긴다."""
    back = _back_edges(nodes, edges)
    forward = [e for e in edges if (e["source"], e["target"]) not in back]

    depth = {n: 0 for n in nodes}
    for _ in range(len(nodes) + 1):
        changed = False
        for e in forward:
            s, t = e["source"], e["target"]
            if s in depth and t in depth and depth[t] < depth[s] + 1:
                depth[t] = depth[s] + 1
                changed = True
        if not changed:
            break

    # __end__는 언제나 맨 아래에 둔다. 중간에 끼면 흐름이 끊겨 보인다.
    if "__end__" in depth:
        depth["__end__"] = max(depth.values()) + 1
    return depth, back


def draw_graph(compiled, title, subtitle="", figsize=(9.5, 8.5)):
    data = compiled.get_graph().to_json()
    nodes = [n["id"] for n in data["nodes"]]
    edges = data["edges"]
    depth, back = _layers(nodes, edges)

    rows = {}
    for n in nodes:
        rows.setdefault(depth[n], []).append(n)

    pos = {}
    max_row = max(rows) if rows else 0
    for d, members in rows.items():
        for i, n in enumerate(members):
            x = (i - (len(members) - 1) / 2) * 4.6
            y = (max_row - d) * 1.9
            pos[n] = (x, y)

    fig, ax = plt.subplots(figsize=figsize)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.axis("off")

    xs = [p[0] for p in pos.values()]
    ys = [p[1] for p in pos.values()]
    ax.set_xlim(min(xs) - 3.6, max(xs) + 3.6)
    ax.set_ylim(min(ys) - 1.4, max(ys) + 1.9)

    ax.text((min(xs) + max(xs)) / 2, max(ys) + 1.45, title, ha="center",
            fontsize=14, color=FG, fontweight="bold")
    if subtitle:
        ax.text((min(xs) + max(xs)) / 2, max(ys) + 1.05, subtitle, ha="center",
                fontsize=9, color=MUTED, style="italic")

    # 되돌아가는 엣지는 왼쪽 바깥으로, 층을 건너뛰는 엣지는 오른쪽 바깥으로
    # 돌린다. 직선으로 두면 사이에 있는 노드 박스를 뚫고 지나가 어느 노드를
    # 거치는 길인지 읽을 수 없다.
    back_seen = 0
    for e in edges:
        s, t = e["source"], e["target"]
        if s not in pos or t not in pos:
            continue
        (x1, y1), (x2, y2) = pos[s], pos[t]
        loop_back = (s, t) in back
        skips = abs(depth[t] - depth[s]) > 1 and not loop_back

        if loop_back:
            back_seen += 1
            rad = -(0.45 + 0.28 * (back_seen - 1))
            p1, p2 = (x1 - 0.5, y1), (x2 - 0.5, y2)
        elif skips:
            rad = 0.4
            p1, p2 = (x1 + 0.5, y1 - 0.25), (x2 + 0.5, y2 + 0.25)
        else:
            rad = 0.0
            p1, p2 = (x1, y1 - 0.3), (x2, y2 + 0.3)

        ax.add_patch(FancyArrowPatch(
            p1, p2, arrowstyle="-|>", mutation_scale=15, linewidth=1.5,
            color="#e06c3a" if loop_back else MUTED,
            linestyle="--" if e.get("conditional") else "-",
            connectionstyle=f"arc3,rad={rad}", zorder=1))

    for n, (x, y) in pos.items():
        color = _kind(n)
        text = n if len(n) <= 30 else n.replace(".", "\n", 1)
        w = max(2.1, 0.135 * max(len(p) for p in text.split("\n")) + 0.7)
        ax.add_patch(FancyBboxPatch((x - w / 2, y - 0.3), w, 0.6,
                                    boxstyle="round,pad=0.07", linewidth=1.8,
                                    edgecolor=color, facecolor=color, alpha=0.2, zorder=2))
        ax.text(x, y, text, ha="center", va="center", fontsize=8.5,
                color=FG, zorder=3, linespacing=1.3)

    legend = [("실선", "항상"), ("점선", "조건부"), ("주황", "되돌아가는 길")]
    ax.text(min(xs) - 3.0, min(ys) - 1.1,
            "   ·   ".join(f"{a} = {b}" for a, b in legend),
            fontsize=8, color=MUTED, ha="left", style="italic")
    plt.tight_layout()
    return fig

class RadixNode:
    __slots__ = ("children", "count", "edge_label")

    def __init__(self, edge_label: str = ""):
        self.edge_label = edge_label
        self.children: dict[str, "RadixNode"] = {}
        self.count: int = 0

    def __getstate__(self):
        return (self.edge_label, self.children, self.count)

    def __setstate__(self, state):
        self.edge_label, self.children, self.count = state


def _common_prefix_len(a: str, b: str) -> int:
    i = 0
    limit = min(len(a), len(b))
    while i < limit and a[i] == b[i]:
        i += 1
    return i


class PrefixTree:
    def __init__(self, max_depth: int = 64, min_count: int = 50):
        self.root = RadixNode()
        self.max_depth = max_depth
        self.min_count = min_count
        self.total_keys = 0

    def insert(self, key: str) -> None:
        self.total_keys += 1
        node = self.root
        node.count += 1
        remaining = key[: self.max_depth]

        while remaining:
            first_char = remaining[0]
            if first_char not in node.children:
                child = RadixNode(edge_label=remaining)
                child.count = 1
                node.children[first_char] = child
                return

            child = node.children[first_char]
            edge = child.edge_label
            common_len = 0
            while (
                common_len < len(edge)
                and common_len < len(remaining)
                and edge[common_len] == remaining[common_len]
            ):
                common_len += 1

            if common_len == len(edge):
                child.count += 1
                remaining = remaining[common_len:]
                node = child
            else:
                split_node = RadixNode(edge_label=edge[:common_len])
                split_node.count = child.count + 1

                child.edge_label = edge[common_len:]
                split_node.children[edge[common_len]] = child

                new_suffix = remaining[common_len:]
                if new_suffix:
                    new_child = RadixNode(edge_label=new_suffix)
                    new_child.count = 1
                    split_node.children[new_suffix[0]] = new_child

                node.children[first_char] = split_node
                return

    def merge(self, other: "PrefixTree") -> None:
        """Merge another PrefixTree into this one by re-inserting all paths with counts."""
        self.total_keys += other.total_keys
        self._replay_subtree(other.root, "")

    def _replay_subtree(self, src_node: RadixNode, prefix: str) -> None:
        """Walk src tree and insert each edge-path with its terminal count delta."""
        for child in src_node.children.values():
            path = prefix + child.edge_label
            # The "own" count at this node = keys that terminate here
            # = child.count - sum(grandchild.count for grandchild in child.children.values())
            child_sum = sum(gc.count for gc in child.children.values())
            own_count = child.count - child_sum
            if own_count > 0:
                self._insert_bulk(path, own_count)
            self._replay_subtree(child, path)

    def _insert_bulk(self, key: str, count: int) -> None:
        """Like insert() but adds `count` instead of 1."""
        node = self.root
        node.count += count
        remaining = key[: self.max_depth]

        while remaining:
            first_char = remaining[0]
            if first_char not in node.children:
                child = RadixNode(edge_label=remaining)
                child.count = count
                node.children[first_char] = child
                return

            child = node.children[first_char]
            edge = child.edge_label
            common_len = _common_prefix_len(edge, remaining)

            if common_len == len(edge):
                child.count += count
                remaining = remaining[common_len:]
                node = child
            else:
                split_node = RadixNode(edge_label=edge[:common_len])
                split_node.count = child.count + count

                child.edge_label = edge[common_len:]
                split_node.children[edge[common_len]] = child

                new_suffix = remaining[common_len:]
                if new_suffix:
                    new_child = RadixNode(edge_label=new_suffix)
                    new_child.count = count
                    split_node.children[new_suffix[0]] = new_child

                node.children[first_char] = split_node
                return

    def prune(self, min_count: int | None = None) -> None:
        threshold = min_count or self.min_count

        def _prune(node: RadixNode):
            to_remove = []
            for ch, child in node.children.items():
                if child.count < threshold:
                    to_remove.append(ch)
                else:
                    _prune(child)
            for ch in to_remove:
                del node.children[ch]

        _prune(self.root)

    def suggest(self, top_n: int = 15, min_coverage_pct: float = 0.5) -> list[dict]:
        min_count = max(int(self.total_keys * min_coverage_pct / 100), 10)
        candidates = []

        def walk(node: RadixNode, prefix: str):
            if node.count < min_count:
                return

            num_children = len(node.children)
            if num_children >= 2 and len(prefix) >= 3:
                score = node.count * (num_children**0.5)
                candidates.append(
                    {
                        "prefix": prefix,
                        "key_count": node.count,
                        "depth": len(prefix),
                        "child_count": num_children,
                        "coverage_pct": round((node.count / self.total_keys) * 100, 2),
                        "score": score,
                    }
                )

            for child in node.children.values():
                walk(child, prefix + child.edge_label)

        for child in self.root.children.values():
            walk(child, child.edge_label)

        candidates.sort(key=lambda x: x["score"], reverse=True)

        result = []
        for c in candidates:
            is_ancestor_of_existing = any(
                r["prefix"].startswith(c["prefix"])
                and r["key_count"] > c["key_count"] * 0.4
                for r in result
            )
            if not is_ancestor_of_existing:
                result.append(c)
            if len(result) >= top_n:
                break

        return result

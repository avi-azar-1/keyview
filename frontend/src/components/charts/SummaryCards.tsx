import { useStore } from "../../store";

export default function SummaryCards() {
  const info = useStore((s) => s.connectionInfo);
  const result = useStore((s) => s.scanResult);

  const estimated = !!result && result.estimate_percent < 100;
  const approx = (v: string) => (estimated ? `~${v}` : v);

  const cards = [
    { label: "Total Keys", value: result ? approx(result.total_keys.toLocaleString()) : "-" },
    { label: "Memory", value: info?.used_memory_human ?? "-" },
    { label: "Clients", value: info?.connected_clients?.toString() ?? "-" },
    ...(info?.cluster_mode
      ? [{ label: "Primary Nodes", value: info.node_count.toString() }]
      : []),
    { label: "Namespaces", value: result ? Object.keys(result.namespace_counts).length.toString() : "-" },
  ];

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      {cards.map((card) => (
        <div
          key={card.label}
          className="bg-white dark:bg-gray-800 rounded-xl p-4 shadow-sm border border-gray-100 dark:border-gray-700"
        >
          <div className="text-sm text-gray-500 dark:text-gray-400">{card.label}</div>
          <div className="text-2xl font-bold text-gray-900 dark:text-white mt-1">
            {card.value}
          </div>
        </div>
      ))}
    </div>
  );
}

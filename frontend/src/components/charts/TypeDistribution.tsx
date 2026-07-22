import ReactECharts from "echarts-for-react";
import { useStore } from "../../store";

const TYPE_COLORS: Record<string, string> = {
  string: "#3b82f6",
  hash: "#8b5cf6",
  list: "#10b981",
  set: "#f59e0b",
  zset: "#ef4444",
  stream: "#06b6d4",
};

export default function TypeDistribution() {
  const result = useStore((s) => s.scanResult);
  const darkMode = useStore((s) => s.darkMode);
  const selectedNamespace = useStore((s) => s.selectedNamespace);

  const typeCounts =
    result && selectedNamespace !== "All"
      ? result.namespace_breakdowns.find((b) => b.namespace === selectedNamespace)
          ?.type_counts ?? {}
      : result?.type_counts ?? {};

  if (!result || Object.keys(typeCounts).length === 0) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-100 dark:border-gray-700 flex items-center justify-center h-80">
        <span className="text-gray-400">Analyzing key types...</span>
      </div>
    );
  }

  const data = Object.entries(typeCounts).map(([name, value]) => ({
    name,
    value,
    itemStyle: { color: TYPE_COLORS[name] || "#6b7280" },
  }));

  const option = {
    title: {
      text: "Key Types",
      left: "center",
      textStyle: { color: darkMode ? "#f3f4f6" : "#111827", fontSize: 14 },
    },
    tooltip: { trigger: "item", formatter: "{b}: {c} ({d}%)" },
    series: [
      {
        type: "pie",
        radius: ["40%", "70%"],
        data,
        label: {
          color: darkMode ? "#d1d5db" : "#374151",
        },
        emphasis: {
          itemStyle: { shadowBlur: 10, shadowOffsetX: 0, shadowColor: "rgba(0,0,0,0.2)" },
        },
      },
    ],
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-100 dark:border-gray-700">
      <ReactECharts option={option} style={{ height: 300 }} />
    </div>
  );
}

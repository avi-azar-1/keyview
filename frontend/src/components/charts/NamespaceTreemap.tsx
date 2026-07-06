import ReactECharts from "echarts-for-react";
import { useStore } from "../../store";

export default function NamespaceTreemap() {
  const result = useStore((s) => s.scanResult);
  const darkMode = useStore((s) => s.darkMode);

  if (!result || Object.keys(result.namespace_counts).length === 0) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-100 dark:border-gray-700 flex items-center justify-center h-80">
        <span className="text-gray-400">Waiting for scan...</span>
      </div>
    );
  }

  const data = Object.entries(result.namespace_counts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .map(([name, value]) => ({ name, value }));

  const option = {
    title: {
      text: "Namespaces (top 20)",
      left: "center",
      textStyle: { color: darkMode ? "#f3f4f6" : "#111827", fontSize: 14 },
    },
    tooltip: { formatter: "{b}: {c} keys" },
    series: [
      {
        type: "treemap",
        data,
        top: 40,
        label: {
          show: true,
          formatter: "{b}\n{c}",
          color: "#fff",
          fontSize: 12,
        },
        breadcrumb: { show: false },
        itemStyle: { borderColor: darkMode ? "#1f2937" : "#fff", borderWidth: 2 },
      },
    ],
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-100 dark:border-gray-700">
      <ReactECharts option={option} style={{ height: 300 }} />
    </div>
  );
}

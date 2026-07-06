import ReactECharts from "echarts-for-react";
import { useStore } from "../../store";

export default function KeyGroupBreakdown() {
  const result = useStore((s) => s.scanResult);
  const darkMode = useStore((s) => s.darkMode);

  if (!result || Object.keys(result.pattern_counts).length === 0) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-100 dark:border-gray-700 flex items-center justify-center h-80">
        <span className="text-gray-400">Add patterns to see grouping</span>
      </div>
    );
  }

  const entries = Object.entries(result.pattern_counts).sort((a, b) => b[1] - a[1]);
  const labels = entries.map(([k]) => k);
  const values = entries.map(([, v]) => v);

  const option = {
    title: {
      text: "Key Groups",
      left: "center",
      textStyle: { color: darkMode ? "#f3f4f6" : "#111827", fontSize: 14 },
    },
    tooltip: { trigger: "axis" },
    xAxis: {
      type: "value",
      axisLabel: { color: darkMode ? "#9ca3af" : "#6b7280" },
      splitLine: { lineStyle: { color: darkMode ? "#374151" : "#f3f4f6" } },
    },
    yAxis: {
      type: "category",
      data: labels,
      axisLabel: { color: darkMode ? "#9ca3af" : "#6b7280" },
      axisLine: { lineStyle: { color: darkMode ? "#4b5563" : "#d1d5db" } },
    },
    series: [
      {
        type: "bar",
        data: values,
        itemStyle: {
          color: "#10b981",
          borderRadius: [0, 4, 4, 0],
        },
      },
    ],
    grid: { top: 50, bottom: 20, left: 120, right: 20 },
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-100 dark:border-gray-700">
      <ReactECharts option={option} style={{ height: 300 }} />
    </div>
  );
}
